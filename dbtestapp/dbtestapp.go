// SPDX-FileCopyrightText: 2021 Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0
//

package main

import (
	"context"
	//"fmt"
	"log"
	"time"
	//"os"

	//"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"

	"github.com/omec-project/MongoDBLibrary"
	"go.mongodb.org/mongo-driver/bson"
)

type Student struct {
	//ID     		primitive.ObjectID 	`bson:"_id,omitempty"`
	Name       string                 `bson:"name,omitempty"`
	Age        int                    `bson:"age,omitempty"`
	Subject    string                 `bson:"subject,omitempty"`
	CreatedAt  time.Time              `bson:"createdAt,omitempty"`
	CustomInfo map[string]interface{} `bson:"customInfo,omitempty"`
}

func iterateChangeStream(routineCtx context.Context, stream *mongo.ChangeStream) {
	log.Println("iterate change stream for timeout")
	defer stream.Close(routineCtx)
	for stream.Next(routineCtx) {
		var data bson.M
		if err := stream.Decode(&data); err != nil {
			panic(err)
		}
		log.Println("iterate stream %v\n", data)
	}
}

func main() {
	log.Println("dbtestapp started")

	// connect to mongoDB
	MongoDBLibrary.SetMongoDB("sdcore", "mongodb://mongodb-headless:27017")

	insertStudentInDB("Osman Amjad", 21)
	insertStudentInDB("Osman Amjad", 21)
	student, err := getStudentFromDB("Osman Amjad")
	if err == nil {
		log.Println("Printing student1")
		log.Println(student)
		log.Println(student.Name)
		log.Println(student.Age)
		log.Println(student.CreatedAt)
	} else {
		log.Println("Error getting student: " + err.Error())
	}

	insertStudentInDB("John Smith", 25)

	// test student that doesn't exist.
	student, err = getStudentFromDB("Nerf Doodle")
	if err == nil {
		log.Println("Printing student2")
		log.Println(student)
		log.Println(student.Name)
		log.Println(student.Age)
		log.Println(student.CreatedAt)
	} else {
		log.Println("Error getting student: " + err.Error())
	}

	log.Println("starting timeout document")
	database := MongoDBLibrary.Client.Database("sdcore")
	timeoutColl := database.Collection("timeout")
	timeoutStream, err := timeoutColl.Watch(context.TODO(), mongo.Pipeline{})
	if err != nil {
		panic(err)
	}
	routineCtx, _ := context.WithCancel(context.Background())
	go iterateChangeStream(routineCtx, timeoutStream)
	createDocumentWithTimeout("yak1")
	//createDocumentWithTimeout("yak2")
	//log.Println("sleeping for 120 seconds")
	//time.Sleep(120 * time.Second)
	//updateDocumentWithTimeout("yak1")
	//deleteDocumentWithTimeout("yak2")

	uniqueId := MongoDBLibrary.GetUniqueIdentity()
	log.Println(uniqueId)

	uniqueId = MongoDBLibrary.GetUniqueIdentity()
	log.Println(uniqueId)

	uniqueId = MongoDBLibrary.GetUniqueIdentityWithinRange(3, 6)
	log.Println(uniqueId)

	uniqueId = MongoDBLibrary.GetUniqueIdentityWithinRange(3, 6)
	log.Println(uniqueId)

	log.Println("TESTING POOL OF IDS")

	MongoDBLibrary.InitializePool("pool1", 10, 32)

	uniqueId, err = MongoDBLibrary.GetIDFromPool("pool1")
	log.Println(uniqueId)

	MongoDBLibrary.ReleaseIDToPool("pool1", uniqueId)

	uniqueId, err = MongoDBLibrary.GetIDFromPool("pool1")
	log.Println(uniqueId)

	uniqueId, err = MongoDBLibrary.GetIDFromPool("pool1")
	log.Println(uniqueId)

	log.Println("TESTING INSERT APPROACH")
	var randomId int32

	randomId, err = MongoDBLibrary.GetIDFromInsertPool("insertApproach")
	log.Println(randomId)
	if err != nil {
		log.Println(err.Error())
	}

	MongoDBLibrary.InitializeInsertPool("insertApproach", 0, 1000, 3)

	randomId, err = MongoDBLibrary.GetIDFromInsertPool("insertApproach")
	log.Println(randomId)
	if err != nil {
		log.Println(err.Error())
	}

	randomId, err = MongoDBLibrary.GetIDFromInsertPool("insertApproach")
	log.Println(randomId)
	if err != nil {
		log.Println(err.Error())
	}

	MongoDBLibrary.ReleaseIDToInsertPool("insertApproach", randomId)

	log.Println("TESTING RETRIES")

	MongoDBLibrary.InitializeInsertPool("testRetry", 0, 6, 3)

	randomId, err = MongoDBLibrary.GetIDFromInsertPool("testRetry")
	log.Println(randomId)
	if err != nil {
		log.Println(err.Error())
	}

	randomId, err = MongoDBLibrary.GetIDFromInsertPool("testRetry")
	log.Println(randomId)
	if err != nil {
		log.Println(err.Error())
	}

	log.Println("TESTING CHUNK APPROACH")
	var lower int32
	var upper int32

	randomId, lower, upper, err = MongoDBLibrary.GetChunkFromPool("studentIdsChunkApproach")
	log.Println(randomId, lower, upper)
	if err != nil {
		log.Println(err.Error())
	}

	MongoDBLibrary.InitializeChunkPool("studentIdsChunkApproach", 0, 1000, 5, 100) // min, max, retries, chunkSize

	randomId, lower, upper, err = MongoDBLibrary.GetChunkFromPool("studentIdsChunkApproach")
	log.Println(randomId, lower, upper)
	if err != nil {
		log.Println(err.Error())
	}

	randomId, lower, upper, err = MongoDBLibrary.GetChunkFromPool("studentIdsChunkApproach")
	log.Println(randomId, lower, upper)
	if err != nil {
		log.Println(err.Error())
	}

	randomId, lower, upper, err = MongoDBLibrary.GetChunkFromPool("studentIdsChunkApproach")
	log.Println(randomId, lower, upper)
	if err != nil {
		log.Println(err.Error())
	}

	MongoDBLibrary.ReleaseChunkToPool("studentIdsChunkApproach", randomId)

	for {
		time.Sleep(100 * time.Second)
	}
}

func getStudentFromDB(name string) (Student, error) {
	var student Student
	filter := bson.M{}
	filter["name"] = name

	result, err := MongoDBLibrary.GetOneCustomDataStructure("student", filter)

	if err == nil {
		bsonBytes, _ := bson.Marshal(result)
		bson.Unmarshal(bsonBytes, &student)

		return student, nil
	}
	return student, err
}

func insertStudentInDB(name string, age int) {
	student := Student{
		Name:      name,
		Age:       age,
		CreatedAt: time.Now(),
	}
	filter := bson.M{}
	_, err := MongoDBLibrary.PutOneCustomDataStructure("student", filter, student)
	if err != nil {
		log.Println("put data failed : ", err)
		return
	}
	_, err = MongoDBLibrary.CreateIndex("student", "Name")
	if err != nil {
		log.Println("Create index failed on Name field.")
	}
}

func deleteDocumentWithTimeout(name string) {
	putData := bson.M{}
	putData["name"] = name
	filter := bson.M{}
	MongoDBLibrary.RestfulAPIDeleteOne("timeout", filter)
}

func createDocumentWithTimeout(name string) {
	putData := bson.M{}
	putData["name"] = name
	putData["customInfo"] = bson.M{"updatedAt": interface{}(time.Now())}
	filter := bson.M{"name": name}
	MongoDBLibrary.RestfulAPIPutOneTimeout("timeout", filter, putData, 60, "customInfo")
}

func updateDocumentWithTimeout(name string) {
	putData := bson.M{}
	putData["name"] = name
	putData["createdAt"] = time.Now()
	filter := bson.M{"name": name}
	MongoDBLibrary.RestfulAPIPatchOneTimeout("timeout", filter, putData, 200, "createdAt")
}
