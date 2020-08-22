package tracer

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path"
	"regexp"
	"strings"
	"syscall"
	"time"

	"github.com/pkg/profile"
	proxy "github.com/shogo82148/go-sql-proxy"
)

var traceID string
var sqlLogFileName string
var sqlLogFile *os.File
var perfomanceLogFileName string
var perfomanceLogFile *os.File
var profilerHandle interface{ Stop() }

// Initialize ISUCON Tracer
// Wait signal (HUP, INT, TERM, QUIT)
func Initialize() {
	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	go func() {
		signal := <-signalCh
		log.Printf("ISUCON Tracer Catch Signal (%s)\n", signal)
		if signal == syscall.SIGHUP {
			Stop()
			Start()
		} else {
			Stop()
			os.Exit(0)
		}
	}()
	Start()
}

// Start ISUCON Tracer Start
func Start() {

	var err error

	if traceID != "" {
		Stop()
	}

	traceID = time.Now().Format("isucon-20060102-150405")
	log.Printf("ISUCON Tracer Start (%s)\n", traceID)

	// Base Log Directory
	logDirName := path.Join("/tmp/isucon/", traceID)
	if err = os.MkdirAll(logDirName, 0755); err != nil {
		log.Fatal(err)
	}

	profilerHandle = profile.Start(profile.ProfilePath(logDirName), profile.NoShutdownHook)

	// Create SQL Log File
	sqlLogFileName = path.Join(logDirName, "sql.log")
	if sqlLogFile, err = os.Create(sqlLogFileName); err != nil {
		log.Printf("ISUCON Tracer Error: %s\n", err.Error())
		return
	}

	// Create Perfomance Log File
	perfomanceLogFileName = path.Join(logDirName, "perfomance.log")
	if perfomanceLogFile, err = os.Create(sqlLogFileName); err != nil {
		log.Printf("ISUCON Tracer Error: %s\n", err.Error())
		return
	}

	rep := regexp.MustCompile(`[ \n\t]{2,}`)

	PreFunc := func(c context.Context, stmt *proxy.Stmt, args []driver.NamedValue) (interface{}, error) {
		return time.Now().UnixNano(), nil
	}
	PostFunc := func(c context.Context, ctx interface{}, stmt *proxy.Stmt, args []driver.NamedValue, err error) error {
		now := time.Now()
		startTime := ctx.(int64)
		timeDelta := now.UnixNano() - startTime
		query := rep.ReplaceAllString(stmt.QueryString, " ")
		if sqlLogFile != nil {
			fmt.Fprintf(sqlLogFile, "%d\t%d\t%s", startTime, timeDelta, query)
		}
		return nil
	}

	for _, driverName := range sql.Drivers() {
		if strings.Contains(driverName, ":logger") {
			continue
		}
		db, _ := sql.Open(driverName, "")
		defer db.Close()
		newDriverName := driverName + ":logger"
		log.Printf("ISUCON Tracer SQL Driver Register: %s\n", newDriverName)
		sql.Register(driverName+":logger", proxy.NewProxyContext(db.Driver(), &proxy.HooksContext{
			PreExec: PreFunc,
			PostExec: func(c context.Context, ctx interface{}, stmt *proxy.Stmt, args []driver.NamedValue, result driver.Result, err error) error {
				return PostFunc(c, ctx, stmt, args, err)
			},
			PreQuery: PreFunc,
			PostQuery: func(c context.Context, ctx interface{}, stmt *proxy.Stmt, args []driver.NamedValue, rows driver.Rows, err error) error {
				return PostFunc(c, ctx, stmt, args, err)
			},
		}))
	}

}

// Stop ISUCON Tracer Stop
func Stop() {
	if traceID != "" {
		traceID = ""
		log.Printf("ISUCON Tracer End (%s)\n", traceID)
	}
	if profilerHandle != nil {
		profilerHandle.Stop()
	}
	if sqlLogFile != nil {
		sqlLogFile.Close()
	}
	if perfomanceLogFile != nil {
		perfomanceLogFile.Close()
	}
}
