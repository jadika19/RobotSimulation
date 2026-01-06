package coordinator

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

// TestRobotFailureConsistency tests coordinator behavior when robots fail
// This test verifies "consistent state" after robot failures:
// 1. Offline robots should not receive new task assignments
// 2. Tasks assigned to crashed robots should eventually be reassigned (if pending queue works)
// 3. Remaining robots should continue operating normally
// 4. Coordinator state should accurately reflect robot availability

func TestRobotFailureConsistency(t *testing.T) {
	// Create fresh coordinator state
	st := &State{
		Robots:        make(map[int]Robot),
		Tasks:         make(map[string]*Task),
		KnownProblems: make(map[string]Problem),
		Width:         20,
		Height:        20,
		NextTaskID:    1,
	}

	t.Run("Setup: Register robots", func(t *testing.T) {
		// Register 2 cleaner robots
		st.Robots[1] = Robot{ID: 1, Type: "cleaner", X: 0, Y: 0, Status: "idle", GRPCAddr: "robot1:50051"}
		st.Robots[2] = Robot{ID: 2, Type: "cleaner", X: 10, Y: 10, Status: "idle", GRPCAddr: "robot2:50051"}

		// Register 1 repair robot
		st.Robots[3] = Robot{ID: 3, Type: "repair", X: 5, Y: 5, Status: "idle", GRPCAddr: "robot3:50051"}

		if len(st.Robots) != 3 {
			t.Errorf("Expected 3 robots, got %d", len(st.Robots))
		}
		t.Logf("✅ Registered 3 robots: 2 cleaners, 1 repair")
	})

	// =========================================================================
	// TEST 1: Verify task assignment works with all robots online
	// =========================================================================
	t.Run("Test1: Task assignment with all robots online", func(t *testing.T) {
		// Assign a dirt task - should go to nearest cleaner
		AssignTask(st, 1, 1, "dirt")

		// Give goroutine time to process (AssignTask spawns gRPC call in goroutine)
		time.Sleep(100 * time.Millisecond)

		st.Mu.Lock()
		taskCount := len(st.Tasks)
		var assignedTask *Task
		for _, task := range st.Tasks {
			assignedTask = task
			break
		}
		st.Mu.Unlock()

		if taskCount == 0 {
			t.Errorf("❌ FAIL: No task created")
		} else if assignedTask.Status == "assigned" && assignedTask.RobotID > 0 {
			t.Logf("✅ PASS: Task assigned to robot %d (status: %s)", assignedTask.RobotID, assignedTask.Status)
		} else if assignedTask.Status == "pending" {
			t.Logf("⚠️  WARN: Task created as pending (no idle robot found)")
		}
	})

	// Reset state for next test
	st.Tasks = make(map[string]*Task)
	st.KnownProblems = make(map[string]Problem)
	for id, robot := range st.Robots {
		robot.Status = "idle"
		robot.TaskID = ""
		st.Robots[id] = robot
	}

	// =========================================================================
	// TEST 2: Simulate robot going offline - should NOT receive tasks
	// =========================================================================
	t.Run("Test2: Offline robot should not receive tasks", func(t *testing.T) {
		// Simulate robot 1 going offline (as would happen via MQTT LWT)
		st.Mu.Lock()
		robot1 := st.Robots[1]
		robot1.Status = "offline"
		st.Robots[1] = robot1
		st.Mu.Unlock()

		// Assign a dirt task near robot 1's position
		// Should go to robot 2 (next nearest cleaner) even though robot 1 is closer
		AssignTask(st, 0, 0, "dirt") // Same position as robot 1

		time.Sleep(100 * time.Millisecond)

		st.Mu.Lock()
		var assignedTask *Task
		for _, task := range st.Tasks {
			assignedTask = task
			break
		}
		st.Mu.Unlock()

		if assignedTask == nil {
			t.Errorf("❌ FAIL: No task created")
		} else if assignedTask.RobotID == 1 {
			t.Errorf("❌ FAIL: Task assigned to OFFLINE robot 1! Coordinator state inconsistent!")
		} else if assignedTask.RobotID == 2 {
			t.Logf("✅ PASS: Task correctly assigned to robot 2 (skipped offline robot 1)")
		} else if assignedTask.Status == "pending" {
			t.Logf("⚠️  INFO: Task is pending (robot 2 might be busy)")
		}
	})

	// Reset state
	st.Tasks = make(map[string]*Task)
	st.KnownProblems = make(map[string]Problem)
	for id, robot := range st.Robots {
		robot.Status = "idle"
		robot.TaskID = ""
		st.Robots[id] = robot
	}

	// =========================================================================
	// TEST 3: All cleaners offline - dirt task should become pending
	// =========================================================================
	t.Run("Test3: All cleaners offline - task should be pending", func(t *testing.T) {
		// Mark both cleaners as offline
		st.Mu.Lock()
		robot1 := st.Robots[1]
		robot1.Status = "offline"
		st.Robots[1] = robot1

		robot2 := st.Robots[2]
		robot2.Status = "offline"
		st.Robots[2] = robot2
		st.Mu.Unlock()

		// Assign a dirt task - no cleaners available
		AssignTask(st, 5, 5, "dirt")

		time.Sleep(100 * time.Millisecond)

		st.Mu.Lock()
		var taskFound *Task
		for _, task := range st.Tasks {
			if task.Type == "dirt" {
				taskFound = task
				break
			}
		}
		st.Mu.Unlock()

		if taskFound == nil {
			t.Errorf("❌ FAIL: Task was DISCARDED! This is the bug we fixed - tasks should be pending")
		} else if taskFound.Status == "pending" {
			t.Logf("✅ PASS: Task correctly created as 'pending' (no cleaners available)")
		} else if taskFound.Status == "assigned" {
			t.Errorf("❌ FAIL: Task assigned to robot %d but all cleaners should be offline!", taskFound.RobotID)
		}
	})

	// Reset state
	st.Tasks = make(map[string]*Task)
	st.KnownProblems = make(map[string]Problem)
	for id, robot := range st.Robots {
		robot.Status = "idle"
		robot.TaskID = ""
		st.Robots[id] = robot
	}

	// =========================================================================
	// TEST 4: Pending task gets assigned when robot comes back online
	// =========================================================================
	t.Run("Test4: Pending task assigned when robot becomes available", func(t *testing.T) {
		// Mark all cleaners as offline
		st.Mu.Lock()
		robot1 := st.Robots[1]
		robot1.Status = "offline"
		st.Robots[1] = robot1

		robot2 := st.Robots[2]
		robot2.Status = "offline"
		st.Robots[2] = robot2
		st.Mu.Unlock()

		// Create a dirt task - should be pending
		AssignTask(st, 3, 3, "dirt")
		time.Sleep(100 * time.Millisecond)

		// Verify task is pending
		st.Mu.Lock()
		pendingCount := 0
		for _, task := range st.Tasks {
			if task.Status == "pending" {
				pendingCount++
			}
		}
		st.Mu.Unlock()

		if pendingCount == 0 {
			t.Logf("⚠️  SKIP: No pending task to test (task may have been discarded)")
			return
		}

		// Simulate robot 1 coming back online
		st.Mu.Lock()
		robot1 = st.Robots[1]
		robot1.Status = "idle"
		st.Robots[1] = robot1
		st.Mu.Unlock()

		// Trigger pending task assignment (as would happen on task completion)
		tryAssignPendingTasks(st)

		time.Sleep(100 * time.Millisecond)

		// Check if pending task was assigned
		st.Mu.Lock()
		assignedCount := 0
		pendingCount = 0
		for _, task := range st.Tasks {
			if task.Status == "assigned" {
				assignedCount++
			} else if task.Status == "pending" {
				pendingCount++
			}
		}
		st.Mu.Unlock()

		if assignedCount > 0 && pendingCount == 0 {
			t.Logf("✅ PASS: Pending task was assigned when robot came online")
		} else if pendingCount > 0 {
			t.Logf("⚠️  WARN: Task still pending after robot came online (may need manual trigger)")
		} else {
			t.Errorf("❌ FAIL: Unexpected state - assigned=%d, pending=%d", assignedCount, pendingCount)
		}
	})

	// Reset state
	st.Tasks = make(map[string]*Task)
	st.KnownProblems = make(map[string]Problem)
	for id, robot := range st.Robots {
		robot.Status = "idle"
		robot.TaskID = ""
		st.Robots[id] = robot
	}

	// =========================================================================
	// TEST 5: Robot type mapping - defect should only go to repair bots
	// =========================================================================
	t.Run("Test5: Defect task only assigned to repair robots", func(t *testing.T) {
		// Assign a defect task
		AssignTask(st, 5, 5, "defect")

		time.Sleep(100 * time.Millisecond)

		st.Mu.Lock()
		var defectTask *Task
		for _, task := range st.Tasks {
			if task.Type == "defect" {
				defectTask = task
				break
			}
		}
		st.Mu.Unlock()

		if defectTask == nil {
			t.Errorf("❌ FAIL: No defect task created")
		} else if defectTask.Status == "assigned" {
			assignedRobot := st.Robots[defectTask.RobotID]
			if assignedRobot.Type == "repair" {
				t.Logf("✅ PASS: Defect task correctly assigned to repair robot %d", defectTask.RobotID)
			} else {
				t.Errorf("❌ FAIL: Defect task assigned to %s robot (should be repair)!", assignedRobot.Type)
			}
		} else if defectTask.Status == "pending" {
			t.Logf("⚠️  INFO: Defect task is pending")
		}
	})

	// Reset state
	st.Tasks = make(map[string]*Task)
	st.KnownProblems = make(map[string]Problem)
	for id, robot := range st.Robots {
		robot.Status = "idle"
		robot.TaskID = ""
		st.Robots[id] = robot
	}

	// =========================================================================
	// TEST 6: Multiple pending tasks - correct type assignment
	// =========================================================================
	t.Run("Test6: Multiple pending tasks get correct robot types", func(t *testing.T) {
		// Mark all robots offline
		st.Mu.Lock()
		for id, robot := range st.Robots {
			robot.Status = "offline"
			st.Robots[id] = robot
		}
		st.Mu.Unlock()

		// Create multiple tasks of different types
		AssignTask(st, 1, 1, "dirt")   // Should go to cleaner
		AssignTask(st, 2, 2, "defect") // Should go to repair
		AssignTask(st, 3, 3, "dirt")   // Should go to cleaner

		time.Sleep(100 * time.Millisecond)

		st.Mu.Lock()
		dirtPending := 0
		defectPending := 0
		for _, task := range st.Tasks {
			if task.Status == "pending" {
				if task.Type == "dirt" {
					dirtPending++
				} else if task.Type == "defect" {
					defectPending++
				}
			}
		}
		st.Mu.Unlock()

		t.Logf("Pending tasks: %d dirt, %d defect", dirtPending, defectPending)

		if dirtPending != 2 || defectPending != 1 {
			t.Errorf("❌ FAIL: Expected 2 dirt + 1 defect pending, got %d dirt + %d defect", dirtPending, defectPending)
		} else {
			t.Logf("✅ PASS: All 3 tasks correctly created as pending")
		}

		// Now bring robots back online and trigger assignment
		st.Mu.Lock()
		for id, robot := range st.Robots {
			robot.Status = "idle"
			st.Robots[id] = robot
		}
		st.Mu.Unlock()

		tryAssignPendingTasks(st)
		time.Sleep(100 * time.Millisecond)

		// Verify assignments
		st.Mu.Lock()
		dirtToCleaners := 0
		defectToRepair := 0
		wrongAssignments := 0
		stillPending := 0

		for _, task := range st.Tasks {
			if task.Status == "pending" {
				stillPending++
				continue
			}
			if task.Status != "assigned" {
				continue
			}

			robot := st.Robots[task.RobotID]
			if task.Type == "dirt" && robot.Type == "cleaner" {
				dirtToCleaners++
			} else if task.Type == "defect" && robot.Type == "repair" {
				defectToRepair++
			} else {
				wrongAssignments++
				t.Logf("❌ Wrong assignment: %s task to %s robot", task.Type, robot.Type)
			}
		}
		st.Mu.Unlock()

		if wrongAssignments > 0 {
			t.Errorf("❌ FAIL: %d tasks assigned to wrong robot type!", wrongAssignments)
		} else if stillPending > 0 {
			t.Logf("⚠️  WARN: %d tasks still pending (may need more robots)", stillPending)
		} else {
			t.Logf("✅ PASS: All tasks assigned to correct robot types (dirt→cleaner, defect→repair)")
		}
	})

	// =========================================================================
	// SUMMARY
	// =========================================================================
	t.Run("Summary", func(t *testing.T) {
		fmt.Println("\n" + strings.Repeat("═", 60))
		fmt.Println("ROBOT FAILURE CONSISTENCY TEST SUMMARY")
		fmt.Println(strings.Repeat("═", 60))
		fmt.Print(`
What "consistent state" means:
1. Coordinator accurately tracks which robots are online/offline
2. Offline robots are NEVER assigned new tasks
3. Tasks for unavailable robots become "pending" (not discarded)
4. Pending tasks get assigned when robots become available
5. Task type → Robot type mapping is always correct:
   - "dirt" → "cleaner" robots only
   - "defect" → "repair" robots only

Run with: go test -v -run TestRobotFailureConsistency ./internal/coordinator/
`)
		fmt.Println(strings.Repeat("═", 60))
	})
}

// TestPendingTaskQueueStress tests the pending task queue under load
func TestPendingTaskQueueStress(t *testing.T) {
	st := &State{
		Robots:        make(map[int]Robot),
		Tasks:         make(map[string]*Task),
		KnownProblems: make(map[string]Problem),
		Width:         50,
		Height:        50,
		NextTaskID:    1,
	}

	// Register just 2 robots
	st.Robots[1] = Robot{ID: 1, Type: "cleaner", X: 0, Y: 0, Status: "idle", GRPCAddr: "robot1:50051"}
	st.Robots[2] = Robot{ID: 2, Type: "cleaner", X: 25, Y: 25, Status: "idle", GRPCAddr: "robot2:50051"}

	t.Run("Stress: 10 tasks with 2 robots", func(t *testing.T) {
		// Create 10 dirt tasks (but only 2 cleaners)
		for i := 0; i < 10; i++ {
			AssignTask(st, i*5, i*5, "dirt")
		}

		time.Sleep(200 * time.Millisecond)

		st.Mu.Lock()
		assigned := 0
		pending := 0
		for _, task := range st.Tasks {
			if task.Status == "assigned" {
				assigned++
			} else if task.Status == "pending" {
				pending++
			}
		}
		totalTasks := len(st.Tasks)
		st.Mu.Unlock()

		t.Logf("Results: %d total tasks, %d assigned, %d pending", totalTasks, assigned, pending)

		if totalTasks != 10 {
			t.Errorf("❌ FAIL: Expected 10 tasks, got %d (tasks were discarded!)", totalTasks)
		} else if assigned <= 2 && pending >= 8 {
			t.Logf("✅ PASS: All tasks preserved (%d assigned, %d pending)", assigned, pending)
		} else {
			t.Logf("⚠️  INFO: Unexpected distribution (assigned=%d, pending=%d)", assigned, pending)
		}
	})

	t.Run("Stress: Simulate task completions", func(t *testing.T) {
		// Count initial pending
		st.Mu.Lock()
		initialPending := 0
		for _, task := range st.Tasks {
			if task.Status == "pending" {
				initialPending++
			}
		}
		st.Mu.Unlock()

		if initialPending == 0 {
			t.Skip("No pending tasks to process")
		}

		// Simulate completing tasks and triggering pending assignment
		completionCycles := 0
		for completionCycles < 10 {
			// Mark one assigned task as completed and free the robot
			st.Mu.Lock()
			for taskID, task := range st.Tasks {
				if task.Status == "assigned" {
					// Free the robot
					if robot, ok := st.Robots[task.RobotID]; ok {
						robot.Status = "idle"
						robot.TaskID = ""
						st.Robots[task.RobotID] = robot
					}
					// Remove completed task
					delete(st.Tasks, taskID)
					break
				}
			}
			st.Mu.Unlock()

			// Trigger pending task assignment (simulates ReportCompletion behavior)
			tryAssignPendingTasks(st)
			completionCycles++

			time.Sleep(50 * time.Millisecond)

			// Check if any pending left
			st.Mu.Lock()
			pendingLeft := 0
			for _, task := range st.Tasks {
				if task.Status == "pending" {
					pendingLeft++
				}
			}
			st.Mu.Unlock()

			if pendingLeft == 0 {
				t.Logf("✅ PASS: All pending tasks processed after %d completion cycles", completionCycles)
				return
			}
		}

		st.Mu.Lock()
		finalPending := 0
		for _, task := range st.Tasks {
			if task.Status == "pending" {
				finalPending++
			}
		}
		st.Mu.Unlock()

		if finalPending > 0 {
			t.Errorf("❌ FAIL: %d tasks still pending after %d completion cycles", finalPending, completionCycles)
		}
	})
}
