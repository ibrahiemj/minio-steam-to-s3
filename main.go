package main

import (
	"bytes"
	"crypto/md5"
	"crypto/sha256"
	"encoding/xml"
	"fmt"
	"hash"
	"io"
	"math"
	"os"
	"sort"

	minio "github.com/minio/minio-go"
)

// optimalPartInfo - calculate the optimal part info for
// a given object size.
//
// NOTE: Assumption here is that for any object to be uploaded to
// any S3 compatible object storage it will have the following
// parameters as constants.
//
const maxPartsCount = 10000
const minPartSize = 1024 * 1024 * 64
const maxMultipartPutObjectSize = 1024 * 1024 * 1024 * 640

func optimalPartInfo(objectSize int64) (totalPartsCount int, partSize int64, lastPartSize int64, err error) {
	// object size is '-1' set it to 640GiB.
	if objectSize == -1 {
		objectSize = maxMultipartPutObjectSize
	}

	// Use floats for part size for all calculations to avoid
	// overflows during float64 to int64 conversions.
	partSizeFlt := math.Ceil(float64(objectSize / maxPartsCount))
	partSizeFlt = math.Ceil(partSizeFlt/minPartSize) * minPartSize

	// Total parts count.
	totalPartsCount = int(math.Ceil(float64(objectSize) / partSizeFlt))

	// Part size.
	partSize = int64(partSizeFlt)

	// Last part size.
	lastPartSize = objectSize - int64(totalPartsCount-1)*partSize
	return totalPartsCount, partSize, lastPartSize, nil
}

// completedParts is a collection of parts sortable by their part numbers.
// used for sorting the uploaded parts before completing the multipart request.
type completedParts []minio.CompletePart

func (a completedParts) Len() int           { return len(a) }
func (a completedParts) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a completedParts) Less(i, j int) bool { return a[i].PartNumber < a[j].PartNumber }

// completeMultipartUpload container for completing multipart upload.
type completeMultipartUpload struct {
	XMLName xml.Name             `xml:"http://s3.amazonaws.com/doc/2006-03-01/ CompleteMultipartUpload" json:"-"`
	Parts   []minio.CompletePart `xml:"Part"`
}

// hashCopyN - Calculates chosen hashes up to partSize amount of bytes.
func hashCopyN(hashAlgorithms map[string]hash.Hash, hashSums map[string][]byte, writer io.Writer, reader io.Reader, partSize int64) (size int64, err error) {
	hashWriter := writer
	for _, v := range hashAlgorithms {
		hashWriter = io.MultiWriter(hashWriter, v)
	}

	// Copies to input at writer.
	size, err = io.CopyN(hashWriter, reader, partSize)
	if err != nil {
		// If not EOF return error right here.
		if err != io.EOF {
			fmt.Println("io.EOF failed")
			return 0, err
		}
	}

	for k, v := range hashAlgorithms {
		hashSums[k] = v.Sum(nil)
	}
	return size, err
}

// PutStream uploads files bigger than 64MiB, and also supports special case where size is unknown i.e '-1'.
func PutStream(bucketName, objectName string, reader io.Reader, metaData map[string][]string) (n int64, err error) {
	ssl := false

	if os.Getenv("SSL") > "" {
		fmt.Println("SSL true")
		ssl = true
	}

	var c minio.Core

	// Instantiate new minio core client object.
	client, err := minio.NewV2(
		os.Getenv("S3_ADDRESS"),
		os.Getenv("ACCESS_KEY"),
		os.Getenv("SECRET_KEY"),
		ssl,
	)
	if err != nil {
		fmt.Println("minio.NewCore failed", err)
		return 0, err
	}

	c.Client = client
	fmt.Println("minio.NewCore OK")

	// Total data read and written to server. should be equal to 'size' at the end of the call.
	var totalUploadedSize int64

	// Complete multipart upload.
	var complMultipartUpload completeMultipartUpload

	// Get the upload id of a previously partially uploaded object or initiate a new multipart upload
	uploadID, err := c.NewMultipartUpload(bucketName, objectName, metaData)
	if err != nil {
		fmt.Println("NewMultipartUpload failed", err)
		return 0, err
	}

	size := int64(-1)

	// Calculate the optimal parts info for a given size.
	totalPartsCount, partSize, _, err := optimalPartInfo(size)
	if err != nil {
		fmt.Println("optimalPartInfo failed")

		return 0, err
	}

	// Initialize parts uploaded map.
	partsInfo := make(map[int]minio.ObjectPart)

	// Part number always starts with '1'.
	partNumber := 1

	// Initialize a temporary buffer.
	tmpBuffer := new(bytes.Buffer)

	for partNumber <= totalPartsCount {
		// Choose hash algorithms to be calculated by hashCopyN, avoid sha256
		// with non-v4 signature request or HTTPS connection
		hashSums := make(map[string][]byte)
		hashAlgos := make(map[string]hash.Hash)
		hashAlgos["md5"] = md5.New()
		hashAlgos["sha256"] = sha256.New()

		// Calculates hash sums while copying partSize bytes into tmpBuffer.
		prtSize, rErr := hashCopyN(hashAlgos, hashSums, tmpBuffer, reader, partSize)
		if rErr != nil && rErr != io.EOF {
			fmt.Println("io.EOF failed")

			return 0, rErr
		}

		// Proceed to upload the part.
		var objPart minio.ObjectPart
		objPart, err = c.PutObjectPart(bucketName, objectName, uploadID, partNumber,
			prtSize, tmpBuffer, hashSums["md5"], hashSums["sha256"])
		if err != nil {
			fmt.Println("PutObjectPart failed")

			// Reset the temporary buffer upon any error.
			tmpBuffer.Reset()
			return totalUploadedSize, err
		}

		// Save successfully uploaded part metadata.
		partsInfo[partNumber] = objPart

		// Reset the temporary buffer.
		tmpBuffer.Reset()

		// Save successfully uploaded size.
		totalUploadedSize += prtSize

		// Increment part number.
		partNumber++

		// For unknown size, Read EOF we break away.
		// We do not have to upload till totalPartsCount.
		if size < 0 && rErr == io.EOF {
			break
		}
	}

	// Verify if we uploaded all the data.
	if size > 0 {
		if totalUploadedSize != size {
			return totalUploadedSize, io.ErrUnexpectedEOF
		}
	}

	// Loop over total uploaded parts to save them in
	// Parts array before completing the multipart request.
	for i := 1; i < partNumber; i++ {
		part, ok := partsInfo[i]
		if !ok {
			fmt.Println("partsInfo failed")
			return 0, fmt.Errorf("Missing part number %d", i)
		}
		complMultipartUpload.Parts = append(complMultipartUpload.Parts,
			minio.CompletePart{
				ETag:       part.ETag,
				PartNumber: part.PartNumber,
			})
	}

	// Sort all completed parts.
	sort.Sort(completedParts(complMultipartUpload.Parts))
	err = c.CompleteMultipartUpload(bucketName, objectName, uploadID, complMultipartUpload.Parts)

	// Return final size.
	return totalUploadedSize, err
}

func main() {
	PutStream("stream-test", "your-object", os.Stdin, map[string][]string{})
}
