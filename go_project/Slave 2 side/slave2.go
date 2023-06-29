package main

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
)

const chunkSize = 1024 * 1024 // 1 MB

func main() {
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		// Get the chunk number from the client
		chunkNumStr := r.URL.Query().Get("chunk")
		chunkNum, err := strconv.Atoi(chunkNumStr)
		if err != nil {
			fmt.Printf("Error converting chunk number %s: %s\n", chunkNumStr, err)
			http.Error(w, "Invalid chunk number", http.StatusBadRequest)
			return
		}

		// Read the chunk from the file
		chunkFile := fmt.Sprintf("chunk%d.txt", chunkNum)
		chunkData, err := ioutil.ReadFile(chunkFile)
		if err != nil {
			fmt.Printf("Error reading chunk %s: %s\n", chunkFile, err)
			http.Error(w, "Internal server error", http.StatusInternalServerError)
			return
		}

		// Send the chunk to the client
		w.Write(chunkData)
	})

	http.ListenAndServe(":32000", nil)

}
