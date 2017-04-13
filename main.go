package main

import (
	"flag"
	"log"
	"os"

	minio "github.com/minio/minio-go"
)

func main() {
	var object, bucket, accessKey, secretKey string

	location := "us-east-1"

	flag.StringVar(&bucket, "b", "stream-test", "Bucket name")
	flag.StringVar(&object, "o", "stream-test", "Object key name")
	flag.StringVar(&accessKey, "a", "", "accessKey")
	flag.StringVar(&secretKey, "s", "", "	")
	flag.Parse()

	client, err := minio.New("127.0.0.1:9000", accessKey, secretKey, false)

	// Test if bucket is there
	exists, _ := client.BucketExists(bucket)
	if !exists {
		// Try to create bucket
		err = client.MakeBucket(bucket, location)
	}

	n, err := client.PutObject(bucket, object, os.Stdin, "stream")
	if err != nil {
		log.Fatal(err)
		return
	}
	log.Printf("Written %d bytes to %s in bucket %s.", n, object, bucket)
}
