package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"log"
	"mime"
	"mime/multipart"
	"net/http"
	"os"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/joho/godotenv"
)

type s3Data struct {
	Key  string
	Size int64
}

type App struct {
	s3Client *s3.S3
	session *session.Session
}

func newApp() App {
	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))
	
	return App{s3Client: s3.New(sess), session: sess} 
}

func uploadFile(file multipart.File) ([]byte, error) {
	buf := bytes.NewBuffer(nil)
	if _, err := io.Copy(buf, file); err != nil {
		return nil, fmt.Errorf("could not upload file. %v", err)
	}

	return buf.Bytes(), nil
}

// listFilesInBucket retrieves and returns all of the files in an S3(-compatible) Bucket
func (app *App)  listFilesInBucket(writer http.ResponseWriter, request *http.Request) {
	request.ParseForm()
	var bucket string = request.FormValue("bucket")

	duration, exists := os.LookupEnv("DURATION")
	if !exists {
		writer.WriteHeader(400)
		writer.Write([]byte("could not retrieve duration, %v"))
		return
	}

	timeout, err := time.ParseDuration(duration)
	if err != nil {
		writer.WriteHeader(400)
		writer.Write([]byte(fmt.Sprintf("could not parse provided duration, %v", err)))
		return
	}
	ctx := context.Background()
	var cancelFn func()
	if timeout > 0 {
		ctx, cancelFn = context.WithTimeout(ctx, timeout)
	}
	if cancelFn != nil {
		defer cancelFn()
	}

	objects := []s3Data{}
	err = app.s3Client.ListObjectsPagesWithContext(
		ctx,
		&s3.ListObjectsInput{Bucket: aws.String(bucket)},
		func(p *s3.ListObjectsOutput, lastPage bool) bool {
			for _, o := range p.Contents {
				objects = append(objects, s3Data{Key: aws.StringValue(o.Key), Size: *aws.Int64(*o.Size)})
			}
			return true
		},
	)
	if err != nil {
		writer.WriteHeader(400)
		writer.Write([]byte(fmt.Sprintf("failed to list objects for bucket, %s, %v", bucket, err)))
		return
	}

	json.NewEncoder(writer).Encode(objects)
	fmt.Printf("successfully retrieved files from bucket: %s.\n", bucket)
}

func (app *App) uploadFileToBucket(writer http.ResponseWriter, request *http.Request) {
	request.ParseForm()

	// Create an uploader with the session and default options
	uploader := s3manager.NewUploader(app.session)

	file, fileMetadata, err := request.FormFile("file")
	if err != nil {
		writer.WriteHeader(400)
		writer.Write([]byte(fmt.Sprintf("could not get file data from request: %v", err)))
		return
	}

	fileData, err := uploadFile(file)
	if err != nil {
		writer.WriteHeader(400)
		writer.Write([]byte(fmt.Sprintf("could not upload file: %v", err)))
		return
	}

	var bucket string = request.FormValue("bucket")

	// Upload the file to S3.
	result, err := uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(fileMetadata.Filename),
		Body:   bytes.NewBuffer(fileData),
	})
	if err != nil {
		writer.WriteHeader(400)
		writer.Write([]byte(fmt.Sprintf("failed to upload file to S3 bucket: %v", err)))
		return
	}

	fmt.Printf("file uploaded to, %s\n", aws.StringValue(&result.Location))
	writer.Write([]byte(fmt.Sprintf("file uploaded to S3 bucket: %s", aws.StringValue(&result.Location))))
}

func (app *App) downloadFileFromBucket(writer http.ResponseWriter, request *http.Request) {
	request.ParseForm()

	var (
		bucket = request.FormValue("bucket")
		downloadFile = request.FormValue("downloadFile")
		file = request.FormValue("file")
	)

	fmt.Printf("Attempting to download %s from bucket: %s\n", file, bucket)
	result, err := app.s3Client.HeadObject(&s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key: aws.String(file),
	})
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			default:
				fmt.Println(aerr.Error())
			}
		} else {
			fmt.Println(err.Error())
		}
		return
	}
	fmt.Printf("File size is %d.\n", *result.ContentLength)

	buf := make([]byte, *result.ContentLength)
	// Create an uploader with the session and default options
	downloader := s3manager.NewDownloader(app.session)
	input := &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key: aws.String(file),
	}
	objectSize, err := downloader.Download(aws.NewWriteAtBuffer(buf), input)
	if err != nil {
		fmt.Printf("Could not download file. Reason: %v.\n", err)
	}
	fmt.Printf("Downloaded file. Size: %d\n", objectSize)

	if (downloadFile == "yes") {
		var fileMode fs.FileMode = 0755
		err = os.WriteFile(file, buf, fileMode)
		if err != nil {
			fmt.Printf("Could not write file to %s\n. Reason: %s", file, err)
		} else {
			fmt.Printf("Wrote file to %s\n", file)
		}

		return
	}

	cd := mime.FormatMediaType("attachment", map[string]string{"filename": file})
	writer.Header().Set("Content-Disposition", cd)
	writer.Header().Set("Content-Type", http.DetectContentType(buf))
	io.Copy(writer, bytes.NewBuffer(buf))
	fmt.Println("Downloaded file.")
}

func main() {
	if err := godotenv.Load(); err != nil {
		log.Print("No .env file found")
	}

	app := newApp()

	// List files in the bucket
	http.HandleFunc("/", app.listFilesInBucket)

	// Upload a file to the bucket
	http.HandleFunc("/upload", app.uploadFileToBucket)

	// Download a file from the bucket
	http.HandleFunc("/download", app.downloadFileFromBucket)

	http.ListenAndServe(":8080", nil)
}
