package cmd

import (
	"encoding/json"
	"fmt"
	"github.com/spf13/cobra"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
)

type VersionByIdResponse struct {
	Subject string `json:"subject"`
	Version int    `json:"version"`
}

var contextName string

func init() {
	getReferenceCmd.Flags().StringVarP(&contextName, "context", "c", "default", "The context name")
	rootCmd.AddCommand(getReferenceCmd)
}

var getReferenceCmd = &cobra.Command{
	Use:   "get-reference",
	Short: "Get reference schema by both id and subject plus version",
	RunE: func(cmd *cobra.Command, args []string) error {
		subjectName := args[0]
		versionId := args[1]

		log.Printf("to find the subjectName: %s with version %s in context %s,\n", subjectName, versionId, contextName)
		srAPIKey := os.Getenv("SCHEMA_REGISTRY_API_KEY")
		srAPISecret := os.Getenv("SCHEMA_REGISTRY_API_SECRET")
		srURL := os.Getenv("SCHEMA_REGISTRY_URL")

		referencedByIds, err := getRefSchemaById(contextName, subjectName, versionId, srAPIKey, srAPISecret, srURL)
		if err != nil {
			log.Fatal(err)
			return err
		}
		outputMap := make(map[string]string)
		for _, id := range referencedByIds {
			schemaId := fmt.Sprintf("%d", id)
			schemaName, version, err := getSubjectAndVersionById(contextName, schemaId, srAPIKey, srAPISecret, srURL)
			if err != nil {
				log.Fatal(err)
				return err
			}
			outputMap[schemaId] = schemaName + "-version-" + version
		}
		prettyPrintMap(outputMap)
		return nil
	},
}

func prettyPrintMap(m map[string]string) {
	jsonBytes, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		log.Fatal(err)
	}
	log.Println(string(jsonBytes))
}

func getRefSchemaById(contextName string, subjectName string, versionId string, srAPIKey string, srAPISecret string, srURL string) ([]int, error) {
	escapedSubjectName := strings.Replace(subjectName, "/", "%2F", -1)

	var url string
	if contextName == "default" {
		url = fmt.Sprintf("%s/subjects/%s/versions/%s/referencedby", srURL, escapedSubjectName, versionId)
	} else {
		url = fmt.Sprintf("%s/contexts/%s/subjects/%s/versions/%s/referencedby", srURL, contextName, escapedSubjectName, versionId)
	}
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		log.Fatal(err)
		return nil, err
	}
	req.SetBasicAuth(srAPIKey, srAPISecret)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Fatal(err)
		return nil, err
	}
	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {

		}
	}(resp.Body)

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	var referencedBy []int
	if err := json.NewDecoder(resp.Body).Decode(&referencedBy); err != nil {
		return nil, err
	}
	return referencedBy, nil
}

func getSubjectAndVersionById(contextName string, schemaId string, srAPIKey string, srAPISecret string, srURL string) (string, string, error) {
	var url string
	if contextName == "default" {
		url = fmt.Sprintf("%s/schemas/ids/%s/versions", srURL, schemaId)
	} else {
		url = fmt.Sprintf("%s/contexts/%s/schemas/ids/%s/versions", srURL, contextName, schemaId)
	}

	req, err := http.NewRequest(http.MethodGet, url, nil)
	req.SetBasicAuth(srAPIKey, srAPISecret)
	if err != nil {
		log.Fatal(err)
		return "", "", err
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Fatal(err)
		return "", "", err
	}

	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {

		}
	}(resp.Body)
	bodyBytes, err := io.ReadAll(resp.Body)

	if err != nil {
		log.Fatal(err)
		return "", "", err
	}
	if resp.StatusCode != http.StatusOK {
		return "", "", fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	var subject []VersionByIdResponse
	err = json.Unmarshal(bodyBytes, &subject)
	if err != nil {
		return "", "", err
	}

	if len(subject) == 0 {
		return "", "", fmt.Errorf("subject is empty")
	}
	return subject[0].Subject, strconv.Itoa(subject[0].Version), nil
}
