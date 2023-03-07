package qdrant_client

import (
	"context"
	"github.com/fatih/structs"
	pb "github.com/qdrant/go-client/qdrant"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Qdrant struct {
	connection *grpc.ClientConn
	collection pb.CollectionsClient
	points     pb.PointsClient
	qdrant     pb.QdrantClient
	snapshot   pb.SnapshotsClient
}

func NewQdrantClient(addr string) (*Qdrant, error) {
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	collection := pb.NewCollectionsClient(conn)
	points := pb.NewPointsClient(conn)
	qdrant := pb.NewQdrantClient(conn)
	snapshot := pb.NewSnapshotsClient(conn)
	return &Qdrant{
		conn,
		collection,
		points,
		qdrant,
		snapshot,
	}, nil
}

func (c *Qdrant) NewCollection(ctx context.Context, name string, vec_size uint64, vec_dist string) error {
	vd := pb.Distance_Euclid
	if vec_dist == "DOT" {
		vd = pb.Distance_Dot
	} else if vec_dist == "COSINE" {
		vd = pb.Distance_Cosine
	}
	_, err := c.collection.Create(ctx, &pb.CreateCollection{
		CollectionName: name,
		VectorsConfig: &pb.VectorsConfig{Config: &pb.VectorsConfig_Params{
			Params: &pb.VectorParams{
				Size:     vec_size,
				Distance: vd,
			},
		}},
	})
	return err
}

func mapPayload(pl interface{}) *pb.Value {
	switch pl := pl.(type) {
	case string:
		return &pb.Value{Kind: &pb.Value_StringValue{StringValue: pl}}
	case uint:
		return &pb.Value{Kind: &pb.Value_IntegerValue{IntegerValue: int64(pl)}}
	case uint8:
		return &pb.Value{Kind: &pb.Value_IntegerValue{IntegerValue: int64(pl)}}
	case uint16:
		return &pb.Value{Kind: &pb.Value_IntegerValue{IntegerValue: int64(pl)}}
	case uint32:
		return &pb.Value{Kind: &pb.Value_IntegerValue{IntegerValue: int64(pl)}}
	case uint64:
		return &pb.Value{Kind: &pb.Value_IntegerValue{IntegerValue: int64(pl)}}
	case int:
		return &pb.Value{Kind: &pb.Value_IntegerValue{IntegerValue: int64(pl)}}
	case int8:
		return &pb.Value{Kind: &pb.Value_IntegerValue{IntegerValue: int64(pl)}}
	case int16:
		return &pb.Value{Kind: &pb.Value_IntegerValue{IntegerValue: int64(pl)}}
	case int32:
		return &pb.Value{Kind: &pb.Value_IntegerValue{IntegerValue: int64(pl)}}
	case int64:
		return &pb.Value{Kind: &pb.Value_IntegerValue{IntegerValue: pl}}
	case float32:
		return &pb.Value{Kind: &pb.Value_DoubleValue{DoubleValue: float64(pl)}}
	case float64:
		return &pb.Value{Kind: &pb.Value_DoubleValue{DoubleValue: pl}}
	case bool:
		return &pb.Value{Kind: &pb.Value_BoolValue{BoolValue: pl}}
	case []interface{}:
		listPayloads := make([]*pb.Value, len(pl))
		for i := range pl {
			listPayloads[i] = mapPayload(pl[i])
		}
		return &pb.Value{Kind: &pb.Value_ListValue{ListValue: &pb.ListValue{Values: listPayloads}}}
	case struct{}:
		plt := structs.Map(&pl)
		payload := make(map[string]*pb.Value, len(plt))
		for j := range plt {
			payload[j] = mapPayload(plt[j])
		}
		return &pb.Value{Kind: &pb.Value_StructValue{StructValue: &pb.Struct{Fields: payload}}}
	case map[string]interface{}:
		payload := make(map[string]*pb.Value, len(pl))
		for j := range pl {
			payload[j] = mapPayload(pl[j])
		}
		return &pb.Value{Kind: &pb.Value_StructValue{StructValue: &pb.Struct{Fields: payload}}}
	default:
		return &pb.Value{Kind: &pb.Value_NullValue{NullValue: pb.NullValue_NULL_VALUE}}
	}
}

func (c *Qdrant) Upsert(ctx context.Context, name string, points [][]float32, payloads []map[string]interface{}, ids []string, wait bool) error {
	pts := make([]*pb.PointStruct, len(points))
	for i, p := range points {
		var plds map[string]*pb.Value
		if payloads != nil {
			plds = make(map[string]*pb.Value)
			for j, pld := range payloads[i] {
				plds[j] = mapPayload(pld)
			}
		}
		pts[i] = &pb.PointStruct{
			Id:      &pb.PointId{PointIdOptions: &pb.PointId_Uuid{Uuid: ids[i]}},
			Payload: plds,
			Vectors: &pb.Vectors{VectorsOptions: &pb.Vectors_Vector{Vector: &pb.Vector{Data: p}}},
		}
	}
	_, err := c.points.Upsert(ctx, &pb.UpsertPoints{
		CollectionName: name,
		Wait:           &wait,
		Points:         pts,
	})
	return err
}

func (c *Qdrant) DeleteCollection(ctx context.Context, name string) error {
	_, err := c.collection.Delete(ctx, &pb.DeleteCollection{
		CollectionName: name,
	})
	return err
}

func (c *Qdrant) DeletePoints(ctx context.Context, name string, ids []string, wait bool) error {
	pointIds := make([]*pb.PointId, len(ids))
	for p := range ids {
		pointIds[p] = &pb.PointId{PointIdOptions: &pb.PointId_Uuid{Uuid: ids[p]}}
	}
	_, err := c.points.Delete(ctx, &pb.DeletePoints{
		CollectionName: name,
		Wait:           &wait,
		Points:         &pb.PointsSelector{PointsSelectorOneOf: &pb.PointsSelector_Points{Points: &pb.PointsIdsList{Ids: pointIds}}},
	})
	return err
}

func getPayload(pld *pb.Value) interface{} {
	switch k := pld.GetKind().(type) {
	case *pb.Value_DoubleValue:
		return k.DoubleValue
	case *pb.Value_IntegerValue:
		return k.IntegerValue
	case *pb.Value_BoolValue:
		return k.BoolValue
	case *pb.Value_StringValue:
		return k.StringValue
	case *pb.Value_ListValue:
		values := make([]interface{}, len(k.ListValue.Values))
		for i, v := range k.ListValue.Values {
			values[i] = getPayload(v)
		}
		return values
	case *pb.Value_StructValue:
		values := make(map[string]interface{})
		for i, v := range k.StructValue.Fields {
			values[i] = getPayload(v)
		}
		return values
	default:
		return nil
	}
}

func getPayloadMap(plds map[string]*pb.Value) map[string]interface{} {
	res := make(map[string]interface{})
	for i, v := range plds {
		res[i] = getPayload(v)
	}
	return res
}

func (c *Qdrant) Search(ctx context.Context, name string, vec []float32, limit uint64) ([]string, [][]float32, []map[string]interface{}, []float32, error) {
	res, err := c.points.Search(ctx, &pb.SearchPoints{
		CollectionName: name,
		Vector:         vec,
		Limit:          limit,
		WithPayload:    &pb.WithPayloadSelector{SelectorOptions: &pb.WithPayloadSelector_Enable{Enable: true}},
	})
	if err != nil {
		return nil, nil, nil, nil, err
	}
	ids := make([]string, len(res.Result))
	vecs := make([][]float32, len(res.Result))
	payloads := make([]map[string]interface{}, len(res.Result))
	scores := make([]float32, len(res.Result))
	for i, r := range res.Result {
		ids[i] = r.Id.GetUuid()
		vecs[i] = r.Vectors.GetVector().GetData()
		payloads[i] = getPayloadMap(r.Payload)
		scores[i] = r.GetScore()
	}
	return ids, vecs, payloads, scores, nil
}

func (c *Qdrant) Health(ctx context.Context) (string, string, error) {
	res, err := c.qdrant.HealthCheck(ctx, &pb.HealthCheckRequest{})
	version := res.GetVersion()
	title := res.GetTitle()
	return title, version, err
}

func (c *Qdrant) TakeSnapShot(ctx context.Context, name string) (string, int64, error) {
	res, err := c.snapshot.Create(ctx, &pb.CreateSnapshotRequest{CollectionName: name})
	return res.GetSnapshotDescription().GetName(), res.GetSnapshotDescription().GetSize(), err
}

func (c *Qdrant) DeleteSnapShot(ctx context.Context, name string, snapName string) error {
	_, err := c.snapshot.Delete(ctx, &pb.DeleteSnapshotRequest{
		CollectionName: name,
		SnapshotName:   snapName,
	})
	return err
}
