package servicebots

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"sync"
	"time"

	"code.fbi.h-da.de/distributed-systems/praktika/lab-for-distributed-systems-ws-2526/burchard/Di1y_2/internal/taskpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// RegResp is the response from coordinator on registration
type RegResp struct {
	ID     int    `json:"id"`
	Type   string `json:"type"`
	Width  int    `json:"width"`
	Height int    `json:"height"`
	Start  struct {
		X int `json:"x"`
		Y int `json:"y"`
	} `json:"start"`
}

// ServiceBot represents a cleaner or repair robot
type ServiceBot struct {
	taskpb.UnimplementedTaskServiceServer
	ID             int
	Type           string // "cleaner" or "repair"
	Status         string // "idle" or "busy"
	X, Y           int
	Width          int
	Height         int
	udpConn        *net.UDPConn
	UDPAddr        string
	CoordGRPCAddr  string // Coordinator gRPC callback address
	Mu             sync.Mutex
	CurrentTask    *taskpb.TaskRequest
	GRPCPort       int
	GRPCListenAddr string
}

// New creates a new service bot with the given type
func New(botType string) *ServiceBot {
	return &ServiceBot{
		Type:   botType,
		Status: "idle",
	}
}

// Register registers the bot with the coordinator via HTTP
func (bot *ServiceBot) Register(coordAddr string, grpcAddr string) error {
	endpoint := "/" + bot.Type + "-robot"
	body := fmt.Sprintf(`{"grpcAddr":"%s"}`, grpcAddr)
	req, err := http.NewRequest("POST", coordAddr+endpoint, bytes.NewBufferString(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("registration failed: %s", resp.Status)
	}

	var regResp RegResp
	if err := json.NewDecoder(resp.Body).Decode(&regResp); err != nil {
		return err
	}

	bot.ID = regResp.ID
	bot.X = regResp.Start.X
	bot.Y = regResp.Start.Y
	bot.Width = regResp.Width
	bot.Height = regResp.Height
	bot.Status = "idle"

	return nil
}

// ConnectUDP opens a UDP connection for position updates
func (bot *ServiceBot) ConnectUDP(udpAddr string) error {
	conn, err := net.Dial("udp", udpAddr)
	if err != nil {
		return err
	}
	bot.udpConn = conn.(*net.UDPConn)
	bot.UDPAddr = udpAddr
	return nil
}

// Close closes the UDP connection
func (bot *ServiceBot) Close() {
	if bot.udpConn != nil {
		bot.udpConn.Close()
	}
}

// SendPosition sends current position to coordinator via UDP
func (bot *ServiceBot) SendPosition() {
	if bot.udpConn == nil {
		return
	}
	// Same format as detector: "id,x,y"
	fmt.Fprintf(bot.udpConn, "%d,%d,%d", bot.ID, bot.X, bot.Y)
}

// AssignTask is called by the coordinator via gRPC
func (bot *ServiceBot) AssignTask(ctx context.Context, req *taskpb.TaskRequest) (*taskpb.TaskResponse, error) {
	bot.Mu.Lock()
	if bot.Status == "busy" {
		bot.Mu.Unlock()
		return &taskpb.TaskResponse{Accepted: false, Message: "robot is busy"}, nil
	}
	bot.Status = "busy"
	bot.CurrentTask = req
	bot.Mu.Unlock()

	log.Printf("gRPC TaskService.AssignTask bot=%d type=%s addr=%s task=%s target=(%d,%d) problem=%s status=accepted", bot.ID, bot.Type, bot.GRPCListenAddr, req.TaskId, req.X, req.Y, req.ProblemType)

	// Execute task in background
	go bot.executeTask(req)

	return &taskpb.TaskResponse{Accepted: true, Message: "task accepted"}, nil
}

func (bot *ServiceBot) executeTask(task *taskpb.TaskRequest) {
	targetX := int(task.X)
	targetY := int(task.Y)

	log.Printf("moving from (%d,%d) to (%d,%d)", bot.X, bot.Y, targetX, targetY)

	// Move to target position step by step
	for bot.X != targetX || bot.Y != targetY {
		if bot.X < targetX {
			bot.X++
		} else if bot.X > targetX {
			bot.X--
		}
		bot.SendPosition()
		time.Sleep(200 * time.Millisecond)

		if bot.Y < targetY {
			bot.Y++
		} else if bot.Y > targetY {
			bot.Y--
		}
		bot.SendPosition()
		time.Sleep(200 * time.Millisecond)
	}

	log.Printf("arrived at (%d,%d), fixing %s...", bot.X, bot.Y, task.ProblemType)

	// Simulate fixing the problem (2 seconds)
	time.Sleep(2 * time.Second)

	log.Printf("fixed %s at (%d,%d)", task.ProblemType, bot.X, bot.Y)

	// Report completion to coordinator
	bot.reportCompletion(task.TaskId, true)

	bot.Mu.Lock()
	bot.Status = "idle"
	bot.CurrentTask = nil
	bot.Mu.Unlock()

	log.Printf("task complete; staying at (%d,%d) and set to idle", bot.X, bot.Y)
}

func (bot *ServiceBot) reportCompletion(taskID string, success bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	log.Printf("gRPC TaskCallback.ReportCompletion dialing=%s bot=%d task=%s", bot.CoordGRPCAddr, bot.ID, taskID)

	conn, err := grpc.DialContext(ctx, bot.CoordGRPCAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("failed to connect to coordinator gRPC: %v", err)
		return
	}
	defer conn.Close()

	client := taskpb.NewTaskCallbackServiceClient(conn)
	resp, err := client.ReportCompletion(ctx, &taskpb.CompletionRequest{
		TaskId:  taskID,
		RobotId: int32(bot.ID),
		Success: success,
	})
	if err != nil {
		log.Printf("failed to report completion: %v", err)
		return
	}
	log.Printf("gRPC TaskCallback.ReportCompletion bot=%d task=%s success=%v acknowledged=%v", bot.ID, taskID, success, resp.Acknowledged)
}

// StartGRPCServer starts the gRPC server for receiving tasks
func (bot *ServiceBot) StartGRPCServer(addr string) error {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	bot.GRPCListenAddr = addr
	logger := func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		start := time.Now()
		resp, err := handler(ctx, req)
		dur := time.Since(start)
		log.Printf("gRPC srv=TaskService bot=%d type=%s addr=%s method=%s dur=%s req=%+v resp=%+v err=%v", bot.ID, bot.Type, bot.GRPCListenAddr, info.FullMethod, dur, req, resp, err)
		return resp, err
	}
	s := grpc.NewServer(grpc.UnaryInterceptor(logger))
	taskpb.RegisterTaskServiceServer(s, bot)
	log.Printf("gRPC server listening on %s", addr)
	return s.Serve(lis)
}

func OpenUDPConnection(addr string) (*net.UDPConn, error) {
	conn, err := net.Dial("udp", addr)
	if err != nil {
		return nil, err
	}
	return conn.(*net.UDPConn), nil
}
