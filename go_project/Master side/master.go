package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
)

const (
	slave1URL = "http://localhost:31000"
	slave2URL = "http://localhost:32000"
)

type ChunkLocation struct {
	SlaveURL string `json:"slave_url"`
	ChunkNum int    `json:"chunk_num"`
}

func main() {
	// Serve the client requests
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		// Get the chunk count from the client
		chunkCountStr := r.URL.Query().Get("chunks")
		chunkCount, err := strconv.Atoi(chunkCountStr)
		if err != nil {
			fmt.Printf("Error converting chunk count %s: %s\n", chunkCountStr, err)
			http.Error(w, "Invalid chunk count", http.StatusBadRequest)
			return
		}

		// Get the locations of the chunks from the slaves
		chunkLocations := make([]ChunkLocation, 0, chunkCount)
		for i := 0; i < chunkCount; i++ {
			chunkLocation := ChunkLocation{ChunkNum: i}
			if i%2 == 0 {
				chunkLocation.SlaveURL = slave1URL
			} else {
				chunkLocation.SlaveURL = slave2URL
			}
			chunkLocations = append(chunkLocations, chunkLocation)
		}

		// Send the locations to the client
		locationsJSON, err := json.Marshal(chunkLocations)
		if err != nil {
			fmt.Printf("Error encoding chunk locations: %s\n", err)
			http.Error(w, "Internal server error", http.StatusInternalServerError)
			return
		}

		w.Write(locationsJSON)

	})

	http.ListenAndServe(":33000", nil)
}
