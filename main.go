package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/jcelliott/lumber"
)

const Version = "1.0.0"

type (
	Logger interface {
		Fatal(string, ...interface{})
		Error(string, ...interface{})
		Warn(string, ...interface{})
		Debug(string, ...interface{})
		Info(string, ...interface{})
		Trace(string, ...interface{})
	}
	Driver struct {
		mutex   sync.Mutex
		mutexes map[string]*sync.Mutex
		dir     string
		log     Logger
	}
)

type Options struct {
	Logger
}

func New(dir string, options *Options) (*Driver, error) {
	dir = filepath.Clean(dir)

	opts := Options{}
	if options != nil {
		opts = *options
	}

	if opts.Logger == nil {
		opts.Logger = lumber.NewConsoleLogger((lumber.INFO))
	}

	driver := Driver{
		dir:     dir,
		mutexes: make(map[string]*sync.Mutex),
		log:     opts.Logger,
	}

	_, err := os.Stat(dir)
	if err == nil {
		opts.Logger.Debug("Using '%s' (database already exists)\n", dir)
		return &driver, nil
	}

	opts.Logger.Debug("Creating the database at '%s'...\n", dir)
	return &driver, os.MkdirAll(dir, 0755)
}

func (d *Driver) Write(collection, resource string, v interface{}) error {
	if collection == "" {
		fmt.Errorf("Missing Collection - no place to save record")
	}

	if resource == "" {
		fmt.Errorf("Missing Resource - nable to save record (no name)")
	}

	mutex := d.getOrCreatedMutex(collection)
	mutex.Lock()
	defer mutex.Unlock()
	dir := filepath.Join(d.dir, collection)
	fnlPath := filepath.Join(dir, resource+".json")
	tmpPath := fnlPath + ".tmp"

	err := os.MkdirAll(dir, 0755)
	if err != nil {
		return err
	}

	b, err := json.MarshalIndent(v, "", "\t")
	if err != nil {
		return err
	}
	b = append(b, byte('\n'))
	err = os.WriteFile(tmpPath, b, 0644)
	if err != nil {
		return err
	}

	return os.Rename(tmpPath, fnlPath)

}

func (d *Driver) Read(collection, resource string, v interface{}) error {
	if collection == "" {
		fmt.Errorf("Missing Collection - unable to read")
	}

	if resource == "" {
		fmt.Errorf("Missing Resource - nable to save record (no name)")
	}

	record := filepath.Join(d.dir, collection, resource)
	_, err := stat(record)
	if err != nil {
		return err
	}

	b, err := os.ReadFile(record + ".json")
	if err != nil {
		return err
	}
	return json.Unmarshal(b, &v)
}

func (d *Driver) ReadAll(collection string) ([]string, error) {
	if collection == "" {
		return nil, fmt.Errorf("Missing Collection - unable to read")
	}
	dir := filepath.Join(d.dir, collection)

	_, err := stat(dir)
	if err != nil {
		return nil, err
	}

	files, _ := os.ReadDir(dir)
	var records []string

	for _, file := range files {
		b, err := os.ReadFile(filepath.Join(dir, file.Name()))
		if err != nil {
			return nil, err
		}
		records = append(records, string(b))
	}
	return records, nil
}

func (d *Driver) Delete(collection, resource string) error {
	path := filepath.Join(collection, resource)
	mutex := d.getOrCreatedMutex(collection)
	mutex.Lock()
	defer mutex.Unlock()

	dir := filepath.Join(d.dir, path)

	switch fi, err := stat(dir); {
	case fi == nil, err != nil:
		return fmt.Errorf("unable to fing file or directory named %v\n", path)

	case fi.Mode().IsDir():
		return os.RemoveAll(dir)

	case fi.Mode().IsRegular():
		return os.RemoveAll(dir + ".json")

	}
	return nil
}

func (d *Driver) getOrCreatedMutex(collection string) *sync.Mutex {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	m, ok := d.mutexes[collection]
	if !ok {
		m = &sync.Mutex{}
		d.mutexes[collection] = m
	}
	return m

}

func stat(path string) (fi os.FileInfo, err error) {
	fi, err = os.Stat(path)
	if os.IsNotExist(err) {
		fi, err = os.Stat(path + ".json")
	}
	return
}

type Address struct {
	City    string
	State   string
	Country string
	Pincode json.Number
}

type User struct {
	Name    string
	Age     json.Number
	Contact string
	Company string
	Address Address
}

func main() {
	dir := "./"
	db, err := New(dir, nil)
	if err != nil {
		fmt.Println("Error:", err)
	}

	employess := []User{
		{"John", "23", "1234567890", "samsung", Address{"Banglore", "karnataka", "India", "560037"}},
		{"paul", "25", "123123334340", "samsung", Address{"bhopal", "Madhya Pradesh", "India", "560031"}},
		{"Vince", "33", "1213317821", "samsung", Address{"Hampi", "karnataka", "India", "71237"}},
		{"Neo", "27", "4342189089", "samsung", Address{"Mysore", "karnataka", "India", "17677"}},
		{"Mark", "21", "433425678921", "samsung", Address{"Delhi", "karnataka", "India", "605037"}},
		{"George", "29", "134134456", "samsung", Address{"Manglore", "karnataka", "India", "560537"}},
	}

	for _, value := range employess {
		db.Write("users", value.Name, User{
			Name:    value.Name,
			Age:     value.Age,
			Contact: value.Contact,
			Company: value.Company,
			Address: value.Address,
		})
	}

	records, err := db.ReadAll("users")
	if err != nil {
		fmt.Println("Error", err)
	}

	fmt.Println(records)

	allusers := []User{}
	for _, f := range records {
		employeeFound := User{}
		err := json.Unmarshal([]byte(f), &employeeFound)
		if err != nil {
			fmt.Println("Error", err)
		}
		allusers = append(allusers, employeeFound)
	}

	fmt.Println("All Users", allusers)

	// err:=db.Delete("users", "john")
	// if err!=nil{
	// 	fmt.Println("Error", err)
	// }

	// err:=db.Delete("users", "")
	// if err!=nil{
	// 	fmt.Println("Error", err)
	// }

}
