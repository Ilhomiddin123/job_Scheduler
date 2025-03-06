package main

import (
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
)

type Job struct {
	ID          string     `json:"id"`
	Description string     `json:"description"`
	ExecuteAt   time.Time  `json:"executeAt"`
	Status      string     `json:"status"`
	ExecutedAt  *time.Time `json:"executedAt,omitempty"`
}

var (
	jobs  = make(map[string]*Job)
	mutex sync.RWMutex
)

func main() {
	r := gin.Default()
	setupRoutes(r)

	go simulateJobs()

	r.Run(":8080")
}

func setupRoutes(r *gin.Engine) {
	r.POST("/jobs", createJob)
	r.GET("/jobs", getJobs)
	r.GET("/jobs/:id", getJobByID)
	r.DELETE("/jobs/:id", cancelJob)
	r.POST("/jobs/:id/run", runJob)
}

func createJob(c *gin.Context) {
	var newJob Job
	if err := c.ShouldBindJSON(&newJob); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid input"})
		return
	}

	if newJob.ExecuteAt.Before(time.Now()) {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Execution time is in the past"})
		return
	}

	newJob.ID = fmt.Sprintf("%d", time.Now().UnixNano())
	newJob.Status = "scheduled"

	mutex.Lock()
	jobs[newJob.ID] = &newJob
	mutex.Unlock()

	log.Printf("Created job with ID: %s", newJob.ID)
	c.JSON(http.StatusCreated, newJob)
}

func getJobs(c *gin.Context) {
	mutex.RLock()
	defer mutex.RUnlock()

	var jobList []*Job
	for _, job := range jobs {
		jobList = append(jobList, job)
	}
	c.JSON(http.StatusOK, jobList)
}

func getJobByID(c *gin.Context) {
	id := c.Param("id")
	job, exists := getJobByIDOrRespond(c, id)
	if !exists {
		return
	}
	c.JSON(http.StatusOK, job)
}

func cancelJob(c *gin.Context) {
	id := c.Param("id")
	mutex.Lock()
	defer mutex.Unlock()

	job, exists := getJobByIDOrRespond(c, id)
	if !exists {
		return
	}
	if job.Status == "executed" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Job already executed"})
		return
	}

	job.Status = "cancelled"
	c.JSON(http.StatusOK, job)
}

func runJob(c *gin.Context) {
	id := c.Param("id")
	mutex.Lock()
	job, exists := jobs[id]
	mutex.Unlock()

	if !exists {
		c.JSON(http.StatusNotFound, gin.H{"error": "Job not found"})
		return
	}
	if job.Status == "executed" || job.Status == "cancelled" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Job cannot be run"})
		return
	}

	simulateExecution(job)
	c.JSON(http.StatusOK, job)
}

func simulateJobs() {
	for {
		<-time.After(1 * time.Second)

		mutex.Lock()
		for _, job := range jobs {
			if job.Status == "scheduled" && job.ExecuteAt.Before(time.Now()) {
				simulateExecution(job)
			}
		}
		mutex.Unlock()
	}
}

func simulateExecution(job *Job) {
	job.Status = "executing"
	now := time.Now()
	job.ExecutedAt = &now
	time.Sleep(1 * time.Second) // Задержка для симуляции выполнения
	job.Status = "executed"

	// Удаление завершенной задачи
	mutex.Lock()
	delete(jobs, job.ID)
	mutex.Unlock()

	log.Printf("Executed job with ID: %s", job.ID)
}

func getJobByIDOrRespond(c *gin.Context, id string) (*Job, bool) {
	mutex.RLock()
	job, exists := jobs[id]
	mutex.RUnlock()
	if !exists {
		c.JSON(http.StatusNotFound, gin.H{"error": "Job not found"})
		return nil, false
	}
	return job, true
}
