package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
)
func write_to_file(m map[string] int){
	// Open the file for writing
	file, err := os.OpenFile("output.txt", os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		panic(err.Error())
	}
	defer file.Close()
	// Write a string to the file
    for k , v := range m {        
        message := fmt.Sprintf("%s : %d\n", k, v)
        _, err = file.WriteString(message)
        if err != nil {
            panic(err.Error())
        }
    }
	// Print a message to the console
	fmt.Println("String written to file")
}
func main() {
    // Get the chunk count from the user
	var chunkCount int
	fmt.Print("Enter chunk count: ")
	fmt.Scan(&chunkCount)

	// Get the locations of the chunks
	locationsURL := fmt.Sprintf("http://localhost:30000/?chunks=%d", chunkCount)
	println(locationsURL)
	dataResp, err := http.Get(locationsURL)
	if err != nil {
		fmt.Printf("Error getting chunk locations: %s\n", err)
		os.Exit(1)
	}

	defer dataResp.Body.Close()
	
	// Parse the chunk locations from the response
	data, err := ioutil.ReadAll(dataResp.Body)
	if err != nil {
		fmt.Printf("Error reading chunk locations: %s\n", err)
		os.Exit(1)
	}

	maps := make(map[string]int)
	err = json.Unmarshal(data, &maps)
	if err != nil {
		fmt.Printf("Error decoding chunk locations: %s\n", err)
		os.Exit(1)
	}
    write_to_file(maps)

	fmt.Println("Done.")
}