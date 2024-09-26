package cmd

import (
	"encoding/json"
	"fmt"
	"github.com/spf13/cobra"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
)

func init() {
	rootCmd.AddCommand(deleteCmd)
}

var deleteCmd = &cobra.Command{
	Use:   "delete-context",
	Short: "Delete all schema id given context",
	RunE: func(cmd *cobra.Command, args []string) error {
		contextName := args[0]
		log.Printf("Deleting context %s", contextName)
		srAPIKey := os.Getenv("SCHEMA_REGISTRY_API_KEY")
		srAPISecret := os.Getenv("SCHEMA_REGISTRY_API_SECRET")
		srURL := os.Getenv("SCHEMA_REGISTRY_URL")

		schemas, err := listAllSchemas(srAPIKey, srAPISecret, srURL)
		if err != nil {
			return err
		}

		filteredSchema, err := filterSchema(contextName, schemas)
		log.Printf("Found %d schemas for context %s\n", len(filteredSchema), contextName)
		if err != nil {
			return err
		}

		var wg sync.WaitGroup
		wg.Add(len(filteredSchema))
		for _, schema := range filteredSchema {
			go deleteSchema(schema, srAPIKey, srAPISecret, srURL, &wg)
		}
		wg.Wait()
		return nil
	},
}

func listAllSchemas(srAPIKey string, srAPISecret string, srURL string) ([]string, error) {
	url := fmt.Sprintf("%s/subjects", srURL)

	req, err := http.NewRequest(http.MethodGet, url, nil)

	if err != nil {
		log.Fatal(err)
		return nil, err
	}
	req.SetBasicAuth(srAPIKey, srAPISecret)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
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

	var subjects []string
	if err := json.NewDecoder(resp.Body).Decode(&subjects); err != nil {
		return nil, err
	}
	return subjects, nil
}

func filterSchema(contextName string, schemas []string) ([]string, error) {
	var output []string

	for _, schema := range schemas {
		if strings.Contains(schema, ":") {
			splitSchema := strings.Split(schema, ":")
			if len(splitSchema) < 3 {
				return nil, fmt.Errorf("invalid schema: %s", schema)
			}
			if splitSchema[1] == contextName {
				output = append(output, schema)
			}
		}
	}
	return output, nil
}

func deleteSchema(subject string, srAPIKey string, srAPISecret string, srURL string, wg *sync.WaitGroup) {
	defer wg.Done()

	url := fmt.Sprintf("%s/subjects/%s", srURL, subject)
	req, err := http.NewRequest(http.MethodDelete, url, nil)
	if err != nil {
		log.Fatal(err)
		return
	}
	req.SetBasicAuth(srAPIKey, srAPISecret)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Fatal(err)
		return
	}

	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {

		}
	}(resp.Body)

	if resp.StatusCode != http.StatusOK {
		log.Printf("unexpected status code: %d for subject: %s\n", resp.StatusCode, subject)
	}
	log.Printf("Deleted subject %s\n", subject)
}
