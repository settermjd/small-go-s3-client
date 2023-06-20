package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/joho/godotenv"
)

type s3Data struct {
	Key  string
	Size int64
}

// listFilesInBucket retrieves and returns all of the files in an S3(-compatible) Bucket
func listFilesInBucket(writer http.ResponseWriter, request *http.Request) {
	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))
	s3Client := s3.New(sess)

	ctx := context.Background()
	var cancelFn func()

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
	if timeout > 0 {
		ctx, cancelFn = context.WithTimeout(ctx, timeout)
	}
	if cancelFn != nil {
		defer cancelFn()
	}

	objects := []s3Data{}
	err = s3Client.ListObjectsPagesWithContext(
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

func main() {
	if err := godotenv.Load(); err != nil {
		log.Print("No .env file found")
	}

	// List files in the bucket
	http.HandleFunc("/", listFilesInBucket)

	// Upload a file to the bucket
	//http.HandleFunc("/upload", uploadFileToBucket)

	// Download a file from the bucket
	//http.HandleFunc("/download", downloadFileFromBucket)

	http.ListenAndServe(":8080", nil)
}
