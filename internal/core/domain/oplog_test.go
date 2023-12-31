package domain

import (
	"reflect"
	"testing"
)

func TestGenerateInsertSQL(t *testing.T) {

	tests := []struct {
		name    string
		oplog   string
		want    []string
		wantErr bool
	}{
		{
			name: "Insert Opereation",
			oplog: `{
					"op": "i",
					"ns": "test.student",
					"o": {
					"_id": "635b79e231d82a8ab1de863b",
					"name": "Selena Miller",
					"roll_no": 51,
					"is_graduated": false,
					"date_of_birth": "2000-01-30"
					}}`,
			want: []string{
				"CREATE SCHEMA test;",
				"CREATE TABLE test.student (_id VARCHAR(255) PRIMARY KEY, date_of_birth VARCHAR(255), is_graduated BOOLEAN, name VARCHAR(255), roll_no FLOAT);",
				"INSERT INTO test.student (_id, date_of_birth, is_graduated, name, roll_no) VALUES ('635b79e231d82a8ab1de863b', '2000-01-30', false, 'Selena Miller', 51);",
			},
			wantErr: false,
		},
		{
			name: "Update Set Opereation",
			oplog: `{
					"op": "u",
					"ns": "test.student",
					"o": {
					"$v": 2,
					"diff": {
						"u": {
							"is_graduated": true
						}
					}
					},
					"o2": {
					"_id": "635b79e231d82a8ab1de863b"
					}}`,
			want:    []string{"UPDATE test.student SET is_graduated = true WHERE _id = '635b79e231d82a8ab1de863b';"},
			wantErr: false,
		},
		{
			name: "Update Multiple Set Opereation",
			oplog: `{
					"op": "u",
					"ns": "test.student",
					"o": {
					"$v": 2,
					"diff": {
						"u": {
							"is_graduated": true,
							"roll_no": 21
						}
					}
					},
					"o2": {
					"_id": "635b79e231d82a8ab1de863b"
					}}`,
			want:    []string{"UPDATE test.student SET is_graduated = true, roll_no = 21 WHERE _id = '635b79e231d82a8ab1de863b';"},
			wantErr: false,
		},
		{
			name: "Update Unset Opereation",
			oplog: `{
					"op": "u",
					"ns": "test.student",
					"o": {
						"$v": 2,
						"diff": {
							"d": {
								"roll_no": false
							}
						}
					},
					"o2": {
						"_id": "635b79e231d82a8ab1de863b"
					}}`,
			want:    []string{"UPDATE test.student SET roll_no = NULL WHERE _id = '635b79e231d82a8ab1de863b';"},
			wantErr: false,
		},
		{
			name: "Delete Opereation",
			oplog: `{
					"op": "d",
					"ns": "test.student",
					"o": {
					"_id": "635b79e231d82a8ab1de863b"
					}}`,
			want:    []string{"DELETE FROM test.student WHERE _id = '635b79e231d82a8ab1de863b';"},
			wantErr: false,
		},
		{
			name: "Insert Operation Multiple Oplg",
			oplog: `[
				{
				  "op": "i",
				  "ns": "test.student",
				  "o": {
					"_id": "635b79e231d82a8ab1de863b",
					"name": "Selena Miller",
					"roll_no": 51,
					"is_graduated": false,
					"date_of_birth": "2000-01-30"
				  }
				},
				{
				  "op": "i",
				  "ns": "test.student",
				  "o": {
					"_id": "14798c213f273a7ca2cf5174",
					"name": "George Smith",
					"roll_no": 21,
					"is_graduated": true,
					"date_of_birth": "2001-03-23"
				  }
				}
			  ]`,
			want: []string{"CREATE SCHEMA test;",
				"CREATE TABLE test.student (_id VARCHAR(255) PRIMARY KEY, date_of_birth VARCHAR(255), is_graduated BOOLEAN, name VARCHAR(255), roll_no FLOAT);",
				"INSERT INTO test.student (_id, date_of_birth, is_graduated, name, roll_no) VALUES ('635b79e231d82a8ab1de863b', '2000-01-30', false, 'Selena Miller', 51);",
				"INSERT INTO test.student (_id, date_of_birth, is_graduated, name, roll_no) VALUES ('14798c213f273a7ca2cf5174', '2001-03-23', true, 'George Smith', 21);"},
			wantErr: false,
		},
		{
			name: "Insert Operation Multiple Oplog With Alter",
			oplog: `[
				{
				  "op": "i",
				  "ns": "test.student",
				  "o": {
					"_id": "635b79e231d82a8ab1de863b",
					"name": "Selena Miller",
					"roll_no": 51,
					"is_graduated": false,
					"date_of_birth": "2000-01-30"
				  }
				},
				{
				  "op": "i",
				  "ns": "test.student",
				  "o": {
					"_id": "14798c213f273a7ca2cf5174",
					"name": "George Smith",
					"roll_no": 21,
					"is_graduated": true,
					"date_of_birth": "2001-03-23",
					"phone": "+91-81254966457"
				  }
				}
			  ]`,
			want: []string{"CREATE SCHEMA test;",
				"CREATE TABLE test.student (_id VARCHAR(255) PRIMARY KEY, date_of_birth VARCHAR(255), is_graduated BOOLEAN, name VARCHAR(255), roll_no FLOAT);",
				"INSERT INTO test.student (_id, date_of_birth, is_graduated, name, roll_no) VALUES ('635b79e231d82a8ab1de863b', '2000-01-30', false, 'Selena Miller', 51);",
				"ALTER TABLE test.student ADD phone VARCHAR(255) ;",
				"INSERT INTO test.student (_id, date_of_birth, is_graduated, name, phone, roll_no) VALUES ('14798c213f273a7ca2cf5174', '2001-03-23', true, 'George Smith', '+91-81254966457', 21);"},
			wantErr: false,
		},
		{
			name: "Nested Documents",
			oplog: `{
					"op": "i",
					"ns": "test.student",
					"o": {
					"_id": "635b79e231d82a8ab1de863b",
					"name": "Selena Miller",
					"roll_no": 51,
					"is_graduated": false,
					"date_of_birth": "2000-01-30",
					"address": [
						{
						"line1": "481 Harborsburgh",
						"zip": "89799"
						},
						{
						"line1": "329 Flatside",
						"zip": "80872"
						}
					],
					"phone": {
						"personal": "7678456640",
						"work": "8130097989"
					}
					}}`,
			want: []string{
				"CREATE SCHEMA test;",
				"CREATE TABLE test.student (_id VARCHAR(255) PRIMARY KEY, date_of_birth VARCHAR(255), is_graduated BOOLEAN, name VARCHAR(255), roll_no FLOAT);",
				"INSERT INTO test.student (_id, date_of_birth, is_graduated, name, roll_no) VALUES ('635b79e231d82a8ab1de863b', '2000-01-30', false, 'Selena Miller', 51);",
				"CREATE TABLE test.student_address (_id VARCHAR(255) PRIMARY KEY, student__id VARCHAR(255), line1 VARCHAR(255), zip VARCHAR(255);",
				"INSERT INTO test.student_address (_id, line1, student__id, zip) VALUES ('64798c213f273a7ca2cf516e', '481 Harborsburgh', '635b79e231d82a8ab1de863b', '89799');",
				"INSERT INTO test.student_address (_id, line1, student__id, zip) VALUES ('14798c213f273a7ca2cf5174', '329 Flatside', '635b79e231d82a8ab1de863b', '80872');",
				"CREATE TABLE test.student_phone (_id VARCHAR(255) PRIMARY KEY, student__id VARCHAR(255), personal VARCHAR(255), work VARCHAR(255));",
				"INSERT INTO test.student_phone (_id, personal, student__id, work) VALUES ('14798c213f273a7ca2cf5199', '7678456640', '635b79e231d82a8ab1de863b', '8130097989');",
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := GenerateSQL(tt.oplog)
			if (err != nil) != tt.wantErr {
				t.Errorf("GenerateSQL() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GenerateSQL() = %v, want %v", got, tt.want)
			}
		})
	}
}
