package msgQClient

import (
	"context"
	"errors"

	"github.com/Brotchu/msgQ/msgqpb/msgq"
	"google.golang.org/grpc"
)

//NewMsgQ : Function to create a new MsgQ struct. msgQ struct is Producer
//who can create a queue with the set name, push messages, and delete the created queue.
func NewMsgQ(name string, addr string) (*msgQ, error) {
	return &msgQ{
		name: name,
		addr: addr,
	}, nil
}

//PingQServer : Function to check if message queue server is running. returns error if not.
func PingQServer(addr string) error {

	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	defer conn.Close()
	if err != nil {
		return err
	}

	req := &msgq.PingRequest{
		Ping: &msgq.Ping{
			Ack: true,
		},
	}

	c := msgq.NewMsgQServiceClient(conn)
	_, err = c.Ping(context.Background(), req)
	return err
}

//Procuder Struct. Associated methods to create , delete and push messages.
type msgQ struct {
	name string
	addr string
}

//CreateQ : Function for producer to create queue with preset name.
//Name is set in msgQ struct created.
//Returns error if queue with same name already created
func (m *msgQ) CreateQ() error {
	conn, err := grpc.Dial(m.addr, grpc.WithInsecure())
	defer conn.Close()
	if err != nil {
		return err
	}

	c := msgq.NewMsgQServiceClient(conn)
	req := &msgq.CreateQRequest{
		Qname: m.name,
	}
	_, err = c.CreateQ(context.Background(), req)
	return err
}

//DeleteQ : Function for producer to delete a queue.
func (m *msgQ) DeleteQ() error {
	conn, err := grpc.Dial(m.addr, grpc.WithInsecure())
	defer conn.Close()
	if err != nil {
		return err
	}

	c := msgq.NewMsgQServiceClient(conn)
	req := &msgq.DeleteQRequest{
		Qname: m.name,
	}
	_, err = c.DeleteQ(context.Background(), req)
	return err
}

//PushMsg : Function for producer to push a message into queue.
//returns error if queue not created already.
func (m *msgQ) PushMsg(msg string, priority int) error {
	if priority > 4 || priority < 0 {
		return errors.New("Error: Priority should be number 1 to 4 ")
	}

	conn, err := grpc.Dial(m.addr, grpc.WithInsecure())
	defer conn.Close()
	if err != nil {
		return err
	}
	c := msgq.NewMsgQServiceClient(conn)

	req := &msgq.AddMessageRequest{
		Qname: m.name,
		Qmsg: &msgq.QMessage{
			Msg:      msg,
			Priority: int32(priority),
		},
	}

	_, err = c.AddMessage(context.Background(), req)
	return err
}

type consumer struct {
	addr string
}

//NewConsumer returns a consumer struct with the specified
//address for message queue server
func NewConsumer(a string) (*consumer, error) {
	return &consumer{
		addr: a,
	}, nil
}

//GetMessage consumes a message from queue.
//queue name passed in as argument
func (n *consumer) GetMessage(q string) (string, error) {

	conn, err := grpc.Dial(n.addr, grpc.WithInsecure())
	defer conn.Close()
	if err != nil {
		return "", err
	}

	c := msgq.NewMsgQServiceClient(conn)

	req := &msgq.GetMessageRequest{
		Qname: q,
	}

	res, err := c.GetMessage(context.Background(), req)
	if err != nil {
		return "", err
	}
	return res.GetQmsg(), nil
}
