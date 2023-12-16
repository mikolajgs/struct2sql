package struct2sql

import (
	"github.com/google/go-github/v56/github"
	"testing"
)

type TestStruct struct {
	IntField    int
	StringField string
	BoolField   bool
}

type TestStruct2 struct {
	IntField2        int
	TestStruct2Field *TestStruct
}

func TestCreateTable(t *testing.T) {
	got := CreateTable(&TestStruct{}, &CreateTableOpts{
		TablePrefix:    "prefix_",
		PrependColumns: "id INTEGER NOT NULL PRIMARY KEY",
		ExcludeFields: map[string]bool{
			"StringField": true,
		},
	})
	want := "CREATE TABLE IF NOT EXISTS prefix_test_structs (id INTEGER NOT NULL PRIMARY KEY,int_field INT,bool_field BOOLEAN);"
	if got != want {
		t.Errorf("got the following: \n%s\n want:\n%s\n", got, want)
	}

	got = CreateTable(&TestStruct2{}, &CreateTableOpts{
		IncludeFields: map[string]bool{
			"TestStruct2Field":  true,
			"TestStruct2Field.": true,
		},
		ExcludeFields: map[string]bool{
			"TestStruct2Field.StringField": true,
		},
	})
	want = "CREATE TABLE IF NOT EXISTS test_struct2s (test_struct2_field_int_field INT NULL,test_struct2_field_bool_field BOOLEAN NULL);"
	if got != want {
		t.Errorf("got the following: \n%s\n want:\n%s\n", got, want)
	}

	got = CreateTable(&github.WorkflowJobEvent{}, &CreateTableOpts{
		PrependColumns: "id INTEGER NOT NULL PRIMARY KEY,time DATETIME NOT NULL,delivery_id TEXT",
		IncludeFields: map[string]bool{
			"WorkflowJob":              true,
			"WorkflowJob.ID":           true,
			"WorkflowJob.RunID":        true,
			"WorkflowJob.RunURL":       true,
			"WorkflowJob.HeadBranch":   true,
			"WorkflowJob.HeadSHA":      true,
			"WorkflowJob.Status":       true,
			"WorkflowJob.Conclusion":   true,
			"WorkflowJob.CreatedAt":    true,
			"WorkflowJob.StartedAt":    true,
			"WorkflowJob.CompletedAt":  true,
			"WorkflowJob.Name":         true,
			"WorkflowJob.RunnerID":     true,
			"WorkflowJob.RunnerName":   true,
			"WorkflowJob.RunAttempt":   true,
			"WorkflowJob.WorkflowName": true,
			"Action":                   true,
			"Repo":                     true,
			"Repo.FullName":            true,
			"Repo.Private":             true,
		},
	})
	want = "CREATE TABLE IF NOT EXISTS workflow_job_events (id INTEGER NOT NULL PRIMARY KEY,time DATETIME NOT NULL,delivery_id TEXT,workflow_job_id INT NULL,workflow_job_run_id INT NULL,workflow_job_run_u_r_l TEXT NULL,workflow_job_head_branch TEXT NULL,workflow_job_head_s_h_a TEXT NULL,workflow_job_status TEXT NULL,workflow_job_conclusion TEXT NULL,workflow_job_created_at DATETIME NULL,workflow_job_started_at DATETIME NULL,workflow_job_completed_at DATETIME NULL,workflow_job_name TEXT NULL,workflow_job_runner_id INT NULL,workflow_job_runner_name TEXT NULL,workflow_job_run_attempt INT NULL,workflow_job_workflow_name TEXT NULL,action TEXT NULL,repo_full_name TEXT NULL,repo_private BOOLEAN NULL);"
	if got != want {
		t.Errorf("got the following: \n%s\n want:\n%s\n", got, want)
	}
}

func TestInsert(t *testing.T) {
	got := Insert(&TestStruct{}, &InsertOpts{
		TablePrefix: "prefix_",
		PrependColumns: "id,",
		PrependValues: "NULL,",
		ExcludeFields: map[string]bool{
			"StringField": true,
		},
	})
	want := "INSERT INTO prefix_test_structs (id,int_field,bool_field) VALUES (NULL,?,?);"
	if got != want {
		t.Errorf("got the following: \n%s\n want:\n%s\n", got, want)
	}

	got = Insert(&TestStruct2{}, &InsertOpts{
		IncludeFields: map[string]bool{
			"TestStruct2Field":  true,
			"TestStruct2Field.": true,
		},
		ExcludeFields: map[string]bool{
			"TestStruct2Field.StringField": true,
		},
	})
	want = "INSERT INTO test_struct2s (test_struct2_field_int_field,test_struct2_field_bool_field) VALUES (?,?);"
	if got != want {
		t.Errorf("got the following: \n%s\n want:\n%s\n", got, want)
	}

	got = Insert(&github.WorkflowJobEvent{}, &InsertOpts{
		PrependColumns: "id,time,delivery_id",
		PrependValues: "NULL,?,?",
		IncludeFields: map[string]bool{
			"WorkflowJob":              true,
			"WorkflowJob.ID":           true,
			"WorkflowJob.RunID":        true,
			"Action":                   true,
			"Repo":                     true,
			"Repo.FullName":            true,
			"Repo.Private":             true,
		},
	})
	want = "INSERT INTO workflow_job_events (id,time,delivery_id,workflow_job_id,workflow_job_run_id,action,repo_full_name,repo_private) VALUES (NULL,?,?,?,?,?,?,?);"
	if got != want {
		t.Errorf("got the following: \n%s\n want:\n%s\n", got, want)
	}
}