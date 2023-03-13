package main // "go install hellopiers.io/ecr-copy@latest"

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"regexp"
	"strings"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/ecr"
)

func main() {

	if len(os.Args) < 4 || len(os.Args) > 5 {
		fmt.Printf("\n  Usage: %s from-repo image-digest-or-tag to-repo [new-tag]\n\n", os.Args[0])
		os.Exit(1)
	}

	sourceRepo := os.Args[1]
	imageDigestOrTag := os.Args[2] // if it's xxx:hex then assume a digest, otherwise a tag
	destRepo := os.Args[3]
	newTag := ""
	if len(os.Args) == 5 {
		newTag = os.Args[4]
	}

	// Credentials to do what we need to do must be available to the SDK in one of
	// the standard ways.
	sess := session.Must(session.NewSession())
	ecrClient := ecr.New(sess)

	manifest, layers, err := getManifest(sourceRepo, imageDigestOrTag, ecrClient)
	if err != nil {
		log.Fatal(err)
	}

	neededLayers, err := checkLayerAvails(destRepo, layers, ecrClient)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("Manifest has %d layers, need to copy %d of them", len(layers), len(neededLayers))

	err = copyLayers(sourceRepo, destRepo, neededLayers, ecrClient)
	if err != nil {
		log.Fatal(err)
	}

	err = putManifest(destRepo, newTag, manifest, ecrClient)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("Copied %s from %s to %s", imageDigestOrTag, sourceRepo, destRepo)
}

func getManifest(sourceRepo, imageDigestOrTag string, ecrClient *ecr.ECR) (*string, []imageLayer, error) {

	input := &ecr.BatchGetImageInput{
		RepositoryName: &sourceRepo,
		ImageIds:       []*ecr.ImageIdentifier{{}},
	}
	if _, hex, didCut := strings.Cut(imageDigestOrTag, ":"); didCut && hexRe.MatchString(hex) {
		input.ImageIds[0].ImageDigest = &imageDigestOrTag
	} else {
		input.ImageIds[0].ImageTag = &imageDigestOrTag
	}

	img, err := ecrClient.BatchGetImage(input)
	if err != nil {
		return nil, nil, fmt.Errorf("BatchGetImage: %w", err)
	}

	if len(img.Images) != 1 {
		return nil, nil, fmt.Errorf("BatchGetImage: Got %d Images", len(img.Images))
	}

	var m manifest
	manifestBytes := []byte(*(img.Images[0].ImageManifest))
	err = json.Unmarshal(manifestBytes, &m)
	if err != nil {
		return nil, nil, fmt.Errorf("Unmarshal ImageManifest: %w", err)
	}

	allLayers := append(m.Layers, m.Config)

	return img.Images[0].ImageManifest, allLayers, nil
}

func checkLayerAvails(destRepo string, layers []imageLayer, ecrClient *ecr.ECR) ([]imageLayer, error) {

	batchInput := &ecr.BatchCheckLayerAvailabilityInput{
		RepositoryName: &destRepo,
	}
	for _, l := range layers {
		l := l // de-alias
		batchInput.LayerDigests = append(batchInput.LayerDigests, &l.Digest)
	}

	avails, err := ecrClient.BatchCheckLayerAvailability(batchInput)
	if err != nil {
		return nil, fmt.Errorf("BatchCheckLayerAvailability: %w", err)
	}

	destHasLayer := map[string]bool{}
	for _, la := range avails.Layers {
		if *la.LayerAvailability == `AVAILABLE` {
			destHasLayer[*la.LayerDigest] = true
		}
	}

	var unavailLayers []imageLayer
	for _, l := range layers {
		if destHasLayer[l.Digest] {
			continue
		}
		unavailLayers = append(unavailLayers, l)
	}

	return unavailLayers, nil
}

func copyLayers(sourceRepo, destRepo string, layers []imageLayer, ecrClient *ecr.ECR) error {

	for _, l := range layers {
		err := copyLayer(sourceRepo, destRepo, l.Digest, ecrClient)
		if err != nil {
			return fmt.Errorf("copyLayer: %w", err)
		}
	}

	return nil
}

func copyLayer(sourceRepo, destRepo string, layerDigest string, ecrClient *ecr.ECR) error {

	dlUrl, err := ecrClient.GetDownloadUrlForLayer(&ecr.GetDownloadUrlForLayerInput{
		RepositoryName: &sourceRepo,
		LayerDigest:    &layerDigest,
	})
	if err != nil {
		return fmt.Errorf("GetDownloadUrlForLayer(%s): %w", layerDigest, err)
	}

	upload, err := ecrClient.InitiateLayerUpload(&ecr.InitiateLayerUploadInput{
		RepositoryName: &destRepo,
	})
	if err != nil {
		return fmt.Errorf("InitiateLayerUpload: %w", err)
	}

	resp, err := http.Get(*dlUrl.DownloadUrl)
	if err != nil {
		return fmt.Errorf("http GET layer(%s): %w", layerDigest, err)
	}
	defer resp.Body.Close()

	b := make([]byte, *upload.PartSize)
	var partFirstByte int64 = 0
	sha := sha256.New()

	log.Printf("Starting upload %s", *upload.UploadId)

	lastPart := false
	for !lastPart {
		partSize := 0

	downloadPart:
		// the GET reader can return less than we want; this inner loop aggregates till we have a full part or it's the end of the layer
		for {
			n, err := resp.Body.Read(b[partSize:])

			// A non EOF error - bail:
			if err != nil && !errors.Is(err, io.EOF) {
				return fmt.Errorf("Read(%s): %w", layerDigest, err)
			}

			// A zero size read
			if n == 0 {
				// With EOF - process what we have
				if errors.Is(err, io.EOF) {
					lastPart = true
					break downloadPart
				}
				// Without EOF - try again
				continue downloadPart
			}

			partSize += n

			// Full buffer - process it
			if partSize == int(*upload.PartSize) {
				break downloadPart
			}
		}

		if partSize == 0 {
			if lastPart {
				// edge case where layer was exactly divisible by the part size?
				break
			}
			panic("internal logic error")
		}

		partLastByte := partFirstByte + int64(partSize) - 1

		log.Printf("Uploading %d bytes from %d to %d", partSize, partFirstByte, partLastByte)

		_, err = ecrClient.UploadLayerPart(&ecr.UploadLayerPartInput{
			LayerPartBlob:  b[:partSize],
			PartFirstByte:  &partFirstByte,
			PartLastByte:   &partLastByte,
			RepositoryName: &destRepo,
			UploadId:       upload.UploadId,
		})
		if err != nil {
			return fmt.Errorf("UploadLayerPart(%s) (%d-%d): %w", layerDigest, partFirstByte, partLastByte, err)
		}

		sha.Write(b[:partSize])

		partFirstByte += int64(partSize)
	}

	uploadDigest := fmt.Sprintf("%s:%064x", *upload.UploadId, sha.Sum(nil))
	layer, err := ecrClient.CompleteLayerUpload(&ecr.CompleteLayerUploadInput{
		RepositoryName: &destRepo,
		UploadId:       upload.UploadId,
		LayerDigests:   []*string{&uploadDigest},
	})
	if err != nil {
		return fmt.Errorf("CompleteLayerUpload(%s): %w", layerDigest, err)
	}

	log.Printf("%#v", layer)

	return nil
}

func putManifest(destRepo, newTag string, manifest *string, ecrClient *ecr.ECR) error {

	input := &ecr.PutImageInput{
		RepositoryName: &destRepo,
		ImageManifest:  manifest,
	}

	if newTag != "" {
		input.ImageTag = &newTag
	}

	o, err := ecrClient.PutImage(input)
	if err != nil {
		return fmt.Errorf("PutImage: %w", err)
	}

	log.Printf("%#v", o)

	return nil
}

type manifest struct {
	Config imageLayer
	Layers []imageLayer
}

type imageLayer struct {
	MediaType string
	Size      int64
	Digest    string
}

var hexRe = regexp.MustCompile(`^[a-f0-9]+$`)
