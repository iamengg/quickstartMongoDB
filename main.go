package main

/*

//MongoDB wiki link for this project
//https://www.mongodb.com/blog/post/quick-start-golang--mongodb--modeling-documents-with-go-data-structures
//https://www.mongodb.com/blog/post/quick-start-golang--mongodb--how-to-create-documents
		Prerequisite -
			MongoDB Atlas with an M0 free cluster ,
			Visual studio Code(SCode),
			MongoDB Go Driver 1.1.2
			Go 1.13

Microservices : https://www.velotio.com/engineering-blog/build-a-containerized-microservice-in-golang

*/
import (
	"context"
	"fmt"
	"log"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

func main() {
	//ConnectingToMongoDB()
	//howToCreateDocs()
	//readingAllDocsFromCollection()
	//howToDeleteDocs()
	workingWithBson()
}

func getURIconnections() string {
	atlas_uri := "mongodb+srv://pratiktest:standard@cluster0.f653n.mongodb.net/myFirstDatabase?retryWrites=true&w=majority"
	fmt.Println("connect string", atlas_uri)
	return atlas_uri
}

func getClientAndCntx() (*mongo.Client, context.Context) {
	client, err := mongo.NewClient(options.Client().ApplyURI(getURIconnections()))
	if err != nil {
		log.Fatal(err)
	}

	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)

	err = client.Connect(ctx)
	if err != nil {
		log.Fatal(err)
	}
	return client, ctx
}

func workingWithBson() {
	client, ctx := getClientAndCntx()

	testDesc := "---------Read docs in testDocCol"
	fmt.Println(testDesc)
	testDB := client.Database("test")
	testDocCol := testDB.Collection("testDoc")

	type Podcast struct {
		ID     primitive.ObjectID `bson:"_id, omitempty"`
		Title  string             `bson:"title, omitempty"`
		Author string             `bson:"author, omitempty"`
		Tags   []string           `bson:"tags, omitempty"`
	}
	type Episode struct {
		ID          primitive.ObjectID `bson:"_id, omitempty"`
		Podcast     primitive.ObjectID `bson:"podcast, omitempty"`
		Title       string             `bson:"title, omitempty"`
		Description string             `bson:"description, omitempty"`
		Duration    int32              `bson:"duration, omitempty"`
	}
	podcast := Podcast{
		Title:  "The polyglot developer",
		Author: "Pratik Shitole",
		Tags:   []string{"dev", "prog", "coding"},
	}

	/*	bson.M{
			"title":  "quick test of MongoDB",
			"author": "From mongoDB openSource",
			"tags":   bson.A{"dev", "prog", "coding"},
		}
	*/
	defer client.Disconnect(ctx)

	db := client.Database("test")
	testDocCol = db.Collection("testDoc")
	cursor, err := testDocCol.Find(ctx, bson.M{"duration": bson.D{{"$gt", 20}}})
	if err != nil {
		log.Fatal(err)
	}
	var episodes []Episode
	if err := cursor.All(ctx, &episodes); err != nil {
		panic(err)
	}
	fmt.Println(episodes)

	insertResult, err := testDocCol.InsertOne(ctx, podcast)
	if err != nil {
		panic(err)
	}
	fmt.Println(insertResult.InsertedID)
}

func howToDeleteDocs() {
	client, ctx := getClientAndCntx()

	testDesc := "---------Read docs in testDocCol"
	fmt.Println(testDesc)
	testDB := client.Database("test")
	testDocCol := testDB.Collection("testDoc")

	cursor, err := testDocCol.Find(ctx, bson.M{"duration": bson.D{{"$gt", 20}}})
	if err != nil {
		log.Fatal(err)
	}

	var data []bson.M
	if err = cursor.All(ctx, &data); err != nil {
		log.Fatal(err)
	}
	fmt.Println("Data at collection " + "testDoc " + " is")
	fmt.Println(data)

	cursor, err = testDocCol.Find(ctx, bson.M{"duration": 25})
	if err != nil {
		log.Fatal(err)
	}

	if err = cursor.All(ctx, &data); err != nil {
		log.Fatal(err)
	}
	fmt.Println("Data at collection " + "testDoc " + " is")
	fmt.Println(data)
	fmt.Println("Try deleting one document")
	res, err := testDocCol.DeleteOne(ctx, bson.M{"duration": 32})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Deleted count is ", res.DeletedCount)

}

func readingAllDocsFromCollection() {
	/*
		https://www.mongodb.com/blog/post/quick-start-golang--mongodb--how-to-read-documents
	*/

	client, ctx := getClientAndCntx()

	testDesc := "---------Read docs in testDocCol"
	fmt.Println(testDesc)
	testDB := client.Database("test")
	testDocCol := testDB.Collection("testDoc")

	cursor, err := testDocCol.Find(ctx, bson.M{})
	if err != nil {
		log.Fatal(err)
	}

	var data []bson.M
	if err = cursor.All(ctx, &data); err != nil {
		log.Fatal(err)
	}
	fmt.Println("Data at collection " + "testDoc " + " is")
	fmt.Println(data)
	testDesc = "---------Test to read single document"
	fmt.Println(testDesc)
	var dataSingleDoc bson.M
	if err = testDocCol.FindOne(ctx, bson.M{}).Decode(&dataSingleDoc); err != nil {
		log.Fatal(err)
	}

	fmt.Println(dataSingleDoc)

	testDesc = "Querying docs from a collection with a filter"
	fmt.Println(testDesc)
	cursor, err = testDocCol.Find(ctx, bson.M{"duration": 25})
	if err != nil {
		log.Fatal(err)
	}
	var d []bson.M
	if err = cursor.All(ctx, &d); err != nil {
		log.Fatal(err)
	}
	fmt.Println(d)

	testDesc = "Querying docs in query"
	fmt.Println(testDesc)
	cursor, err = testDocCol.Find(ctx,
		bson.D{{"duration", bson.D{{"$gt", 24}}}})
	if err != nil {
		log.Fatal(err)
	}
	//var d []bson.M
	if err = cursor.All(ctx, &d); err != nil {
		log.Fatal(err)
	}
	fmt.Println(d)

	testDesc = "Querying docs in query"
	fmt.Println(testDesc)
	opts := options.Find()
	opts.SetSort(bson.D{{"duration", -1}})
	cursor, err = testDocCol.Find(ctx,
		bson.D{{"duration", bson.D{{"$gt", 24}}}}, opts)
	if err != nil {
		log.Fatal(err)
	}
	//var d []bson.M
	if err = cursor.All(ctx, &d); err != nil {
		log.Fatal(err)
	}
	fmt.Println(d)

}

func ConnectingToMongoDB() {
	client, ctx := getClientAndCntx()

	err := client.Ping(ctx, readpref.Primary())
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("client is **reachable**")
	databases, err := client.ListDatabaseNames(ctx, bson.M{})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(databases)
	sampleTrainingDB := client.Database("sample_training")
	tripsColl := sampleTrainingDB.Collection("trips")
	fmt.Println(tripsColl)
	defer client.Disconnect(ctx)
	fmt.Println("Disconnect client")
}

func howToCreateDocs() {

	client, ctx := getClientAndCntx()

	err := client.Ping(ctx, readpref.Primary())
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("client is **reachable**")
	databases, err := client.ListDatabaseNames(ctx, bson.M{})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(databases)
	sampleTrainingDB := client.Database("test")
	testDocColl := sampleTrainingDB.Collection("testDoc")
	fmt.Println(testDocColl)
	testDocCollRes, err := testDocColl.InsertOne(ctx, bson.D{
		{Key: "title", Value: "The Polyglot Developer Podcast"},
		{Key: "author", Value: "Nic Raboy"},
		{Key: "tags", Value: bson.A{"development", "programming", "coding"}},
	})

	episodeResult, err := testDocColl.InsertMany(ctx, []interface{}{
		bson.D{
			{"podcast", testDocCollRes.InsertedID},
			{"title", "GraphQL for API Development"},
			{"description", "Learn about GraphQL from the co-creator of GraphQL, Lee Byron."},
			{"duration", 25},
		},
		bson.D{
			{"podcast", testDocCollRes.InsertedID},
			{"title", "One"},
			{"description", "One num."},
			{"duration", 1},
		},
		bson.D{
			{"podcast", testDocCollRes.InsertedID},
			{"title", "Two"},
			{"description", "Two num."},
			{"duration", 2},
		},
		bson.D{
			{"podcast", testDocCollRes.InsertedID},
			{"title", "Three"},
			{"description", "Three num."},
			{"duration", 3},
		},
		bson.D{
			{"podcast", testDocCollRes.InsertedID},
			{"title", "Four"},
			{"description", "Four num."},
			{"duration", 4},
		},
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Inserted %v documents into episode collection!\n", len(episodeResult.InsertedIDs))
	defer client.Disconnect(ctx)
	fmt.Println("Disconnect client")
}
