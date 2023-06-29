package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"

	_ "github.com/go-sql-driver/mysql"
)

type ChunkLocation struct {
	SlaveURL string `json:"slave_url"`
	ChunkNum int    `json:"chunk_num"`
}

func select_groupBy_DB() map[string]int {
	// Open a connection to the MySQL database
	db, err := sql.Open("mysql", "root:sqlr--tpass33@tcp(127.0.0.1:3306)/go_project")
	if err != nil {
		panic(err.Error())
	}
	defer db.Close()

	// Execute the SELECT query with a GROUP BY clause
	rows, err := db.Query("SELECT letter, SUM(count) FROM frequency GROUP BY letter")
	if err != nil {
		panic(err.Error())
	}
	defer rows.Close()

	// Iterate over the rows returned by the query and print them to the console
	reduceLetters := make(map[string]int);
	for rows.Next() {
		var letter string
		var count int
		err := rows.Scan(&letter, &count)
		if err != nil {
			panic(err.Error())
		}
		//fmt.Printf("%s: %d\n", letter, count)
		reduceLetters[letter] += count
	}
	return reduceLetters
}
func toStringList( chunk []byte ) []string {
	// Declare an array of bytes
	//bytes := []byte("foo,bar,baz")

	// Split the bytes into an array of strings using the "," separator
	strings := bytes.Split(chunk, []byte(" "))

	// Print the array of strings to the console
	stringSlice := make([]string, 0)
	for _, s := range strings {
		stringSlice = append(stringSlice, string(s))
	}
	return stringSlice
}
func MapReduce(input []string) map[string]int {    
    // Map function
    mapper := func(str string) []string {
        return strings.Split(str, "")
    }
    
    // Reduce function
    reducer := func(key string, values []string) int {
        return len(values)
    }
    
    // Map phase
    intermediate := make(map[string][]string)
    for _, str := range input {
        for _, word := range mapper(str) {
            intermediate[word] = append(intermediate[word], str)
        }
    }
    
    // Reduce phase
    output := make(map[string]int)
    for key, values := range intermediate {
        output[key] = reducer(key, values)
    }
    
    return output // Output: map[a:3 b:2 c:1 e:2 h:2 l:3 n:2 p:3 r:2 y:1]
}
func insert_db( letMap map[string]int ){
	// Open a connection to the MySQL database
	db, err := sql.Open("mysql", "root:sqlr--tpass33@tcp(127.0.0.1:3306)/go_project")
	if err != nil {
		panic(err.Error())
	}
	defer db.Close()

	// Prepare a statement for inserting data into the "users" table
	stmt, err := db.Prepare("INSERT INTO frequency(letter, count) VALUES(?, ?)")
	if err != nil {
		panic(err.Error())
	}
	defer stmt.Close()

	// Execute the statement with the values for name and email
	for k, v := range letMap {
		_, err = stmt.Exec(k, v)
		if err != nil {
			panic(err.Error())
		}
	}
	fmt.Println("Data inserted successfully!")
}

func main() {
	// Serve the client requests
	http.HandleFunc("/", func( w http.ResponseWriter, r *http.Request) {
		// Get the chunk count from the client
		chunkCountStr := r.URL.Query().Get("chunks") 
		chunkCount, err := strconv.Atoi(chunkCountStr)
		if err != nil {
			fmt.Printf("Error converting chunk count %s: %s\n", chunkCountStr, err)
			http.Error(w, "Invalid chunk count", http.StatusBadRequest)
			return
		}

		// Get the locations of the chunks
		locationsURL := fmt.Sprintf("http://localhost:33000/?chunks=%d", chunkCount)
		println(locationsURL)
		locationsResp, err := http.Get(locationsURL)
		if err != nil {
			fmt.Printf("Error getting chunk locations: %s\n", err)
			os.Exit(1)
		}

		defer locationsResp.Body.Close()
		
		// Parse the chunk locations from the response
		locationsData, err := ioutil.ReadAll(locationsResp.Body)
		if err != nil {
			fmt.Printf("Error reading chunk locations: %s\n", err)
			os.Exit(1)
		}

		chunkLocations := make([]ChunkLocation, 0, chunkCount)
		err = json.Unmarshal(locationsData, &chunkLocations)
		if err != nil {
			fmt.Printf("Error decoding chunk locations: %s\n", err)
			os.Exit(1)
		}
		

		// Download the chunks from the slaves
		var wg sync.WaitGroup
		chunkData := make([][]byte , chunkCount) //[ [file1]  , [file2] ]

		for _ , chunkLocation := range chunkLocations {
			wg.Add(1)
			go func(chunkLocation ChunkLocation) {
				defer wg.Done()

				chunkURL := fmt.Sprintf("%s/?chunk=%d", chunkLocation.SlaveURL, chunkLocation.ChunkNum)
				chunkResp, err := http.Get(chunkURL)
				if err != nil {
					fmt.Printf("Error getting chunk %d from %s: %s\n", chunkLocation.ChunkNum, chunkLocation.SlaveURL, err)
					return
				}
				defer chunkResp.Body.Close()
		
				chunkData[chunkLocation.ChunkNum], err = ioutil.ReadAll(chunkResp.Body)
				if err != nil {
					fmt.Printf("Error reading chunk %d from %s: %s\n", chunkLocation.ChunkNum, chunkLocation.SlaveURL, err)
					return
				}
			}(chunkLocation)
		}
		wg.Wait()
		

		// insert the chunks to db
		var wg2 sync.WaitGroup	
		for _ , chunk := range chunkData {
			wg2.Add(1)
			go func(_chunk []byte) {
				defer wg2.Done()
				strings := toStringList(_chunk)
				maps := MapReduce(strings)
				insert_db(maps)
			}(chunk)
		}
		wg2.Wait()
		fmt.Println("File downloaded and processed successfully.")

		mps := select_groupBy_DB();

		// Send the locations to the client
		dataJSON, err := json.Marshal(mps)
		if err != nil {
			fmt.Printf("Error encoding chunk locations: %s\n", err)
			http.Error(w, "Internal server error", http.StatusInternalServerError)
			return
		}
		w.Write(dataJSON)
	})

	http.ListenAndServe(":30000", nil)
}
