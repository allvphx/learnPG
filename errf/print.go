package errf

import (
	"encoding/json"
	"fmt"
	"log"
)

func AsJson(v any) string {
	jsonBytes, err := json.Marshal(v)
	if err != nil {
		log.Fatalf("Error occurred during marshaling. Error: %s", err.Error())
	}

	// Convert bytes to string and print
	jsonString := string(jsonBytes)
	return jsonString
}

func Jprint(v any) {
	// Convert v to a JSON string
	jsonBytes, err := json.Marshal(v)
	if err != nil {
		log.Fatalf("Error occurred during marshaling. Error: %s", err.Error())
	}

	// Convert bytes to string and print
	jsonString := string(jsonBytes)
	fmt.Println(jsonString)
}
