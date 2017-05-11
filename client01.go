// This example comes from here:
//
// https://github.com/dgraph-io/dgraph/blob/master/wiki/content/clients/index.md
//

package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	"google.golang.org/grpc"

	"github.com/dgraph-io/dgraph/client"
	"github.com/dgraph-io/dgraph/protos"
	"github.com/gogo/protobuf/proto"
	"github.com/twpayne/go-geom/encoding/wkb"
)

var (
	dgraph = flag.String("d", "127.0.0.1:8080", "Dgraph server address")
)

func main() {
	conn, err := grpc.Dial(*dgraph, grpc.WithInsecure())

	c := protos.NewDgraphClient(conn)
	req := client.Req{}

	// _:person1 tells Dgraph to assign a new Uid and is the preferred way of creating new nodes.
	// See https://docs.dgraph.io/master/query-language/#assigning-uid for more details.
	nq := protos.NQuad{
		Subject:   "_:person1",
		Predicate: "name",
	}
	client.Str("Steven Spielberg", &nq)

	if err := client.AddFacet("since", "2006-01-02T15:04:05", &nq); err != nil {
		log.Fatal(err)
	}

	// To add a facet of type string, use a raw string literal with "" like below or if
	// you are using an interpreted string literal then you'd need to add and escape the
	// double quotes like client.AddFacet("alias","\"Steve\"", &nq)
	if err := client.AddFacet("alias", `"Steve"`, &nq); err != nil {
		log.Fatal(err)
	}

	req.AddMutation(nq, client.SET)

	nq = protos.NQuad{
		Subject:   "_:person1",
		Predicate: "now",
	}
	if err = client.Datetime(time.Now(), &nq); err != nil {
		log.Fatal(err)
	}
	req.AddMutation(nq, client.SET)

	nq = protos.NQuad{
		Subject:   "_:person1",
		Predicate: "birthday",
	}
	if err = client.Date(time.Date(1991, 2, 1, 0, 0, 0, 0, time.UTC), &nq); err != nil {
		log.Fatal(err)
	}
	req.AddMutation(nq, client.SET)

	nq = protos.NQuad{
		Subject:   "_:person1",
		Predicate: "loc",
	}
	if err = client.ValueFromGeoJson(`{"type":"Point","coordinates":[-122.2207184,37.72129059]}`, &nq); err != nil {
		log.Fatal(err)
	}
	req.AddMutation(nq, client.SET)

	nq = protos.NQuad{
		Subject:   "_:person1",
		Predicate: "age",
	}
	if err = client.Int(25, &nq); err != nil {
		log.Fatal(err)
	}
	req.AddMutation(nq, client.SET)

	nq = protos.NQuad{
		Subject:   "_:person1",
		Predicate: "salary",
	}
	if err = client.Float(13333.6161, &nq); err != nil {
		log.Fatal(err)
	}
	req.AddMutation(nq, client.SET)

	nq = protos.NQuad{
		Subject:   "_:person1",
		Predicate: "married",
	}
	if err = client.Bool(false, &nq); err != nil {
		log.Fatal(err)
	}
	req.AddMutation(nq, client.SET)

	nq = protos.NQuad{
		Subject:   "_:person2",
		Predicate: "name",
	}
	client.Str("William Jones", &nq)
	req.AddMutation(nq, client.SET)

	// Lets connect the two nodes together.
	nq = protos.NQuad{
		Subject:   "_:person1",
		Predicate: "friend",
		ObjectId:  "_:person2",
	}

	if err := client.AddFacet("close", "true", &nq); err != nil {
		log.Fatal(err)
	}

	req.AddMutation(nq, client.SET)
	// Lets run the request with all these mutations.
	resp, err := c.Run(context.Background(), req.Request())
	if err != nil {
		log.Fatalf("Error in getting response from server, %s", err)
	}
	person1Uid := resp.AssignedUids["person1"]
	person2Uid := resp.AssignedUids["person2"]

	// Lets initiate a new request and query for the data.
	req = client.Req{}
	// Lets set the starting node id to person1Uid.
	req.SetQuery(fmt.Sprintf(`{
		me(id: %v) {
			_uid_
			name @facets
			now
			birthday
			loc
			salary
			age
			married
			friend @facets {
				_uid_
				name
			}
		}
	}`, client.Uid(person1Uid)))
	resp, err = c.Run(context.Background(), req.Request())
	if err != nil {
		log.Fatalf("Error in getting response from server, %s", err)
	}

	fmt.Printf("Raw Response: %+v\n", proto.MarshalTextString(resp))

	person1 := resp.N[0].Children[0]
	props := person1.Properties
	name := props[0].Value.GetStrVal()
	fmt.Println("Name: ", name)

	// We use time.Parse for Date and Datetime values, to get the actual value back.
	now, err := time.Parse(time.RFC3339, props[1].Value.GetStrVal())
	if err != nil {
		log.Fatalf("Error in parsing time, %s", err)
	}
	fmt.Println("Now: ", now)

	birthday, err := time.Parse(time.RFC3339, props[2].Value.GetStrVal())
	if err != nil {
		log.Fatalf("Error in parsing time, %s", err)
	}
	fmt.Println("Birthday: ", birthday)

	// We use wkb.Unmarshal to get the geom object back from Geo val.
	geom, err := wkb.Unmarshal(props[3].Value.GetGeoVal())
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Loc: ", geom)

	fmt.Println("Salary: ", props[4].Value.GetDoubleVal())
	fmt.Println("Age: ", props[5].Value.GetIntVal())
	fmt.Println("Married: ", props[6].Value.GetBoolVal())

	person2 := person1.Children[0]
	fmt.Printf("%v name: %v\n", person2.Attribute, person2.Properties[0].Value.GetStrVal())

	// Deleting an edge.
	nq = protos.NQuad{
		Subject:   client.Uid(person1Uid),
		Predicate: "friend",
		ObjectId:  client.Uid(person2Uid),
	}
	req = client.Req{}
	req.AddMutation(nq, client.DEL)
	resp, err = c.Run(context.Background(), req.Request())
	if err != nil {
		log.Fatalf("Error in getting response from server, %s", err)
	}
}
