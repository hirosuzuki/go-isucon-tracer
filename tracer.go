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

// TraceID is unique trace ID
var TraceID string

var sqlLogFileName string
var sqlLogFile *os.File
var perfomanceLogFileName string
var perfomanceLogFile *os.File
var webrouteLogFileName string
var webrouteLogFile *os.File
var profilerHandle interface{ Stop() }

// PerfHandle is Perfomance Measure Handle
type PerfHandle struct {
	startTime int64
	tag       string
	text      string
	toFile    *os.File
}

// End is Function called when Perfomance Measure End
func (p *PerfHandle) End() {
	if p.toFile != nil {
		timeDelta := time.Now().UnixNano() - p.startTime
		fmt.Fprintf(p.toFile, "%d\t%d\t%s\t%s\n", p.startTime, timeDelta, p.tag, p.text)
	}
}

// Measure make create New Performance Measure Handle
func Measure(tag string, text string) PerfHandle {
	return PerfHandle{startTime: time.Now().UnixNano(), tag: tag, text: text, toFile: perfomanceLogFile}
}

// WebRouteMeasure make create New Web Route Performance Measure Handle
func WebRouteMeasure(tag string, text string) PerfHandle {
	return PerfHandle{startTime: time.Now().UnixNano(), tag: tag, text: text, toFile: webrouteLogFile}
}

// Initialize ISUCON Tracer
// Wait signal (USR1, USR2, HUP, INT, TERM, QUIT)
func init() {
	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, syscall.SIGUSR1, syscall.SIGUSR2, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	go func() {
		for {
			signal := <-signalCh
			log.Printf("ISUCON Tracer Catch Signal (%s)\n", signal)
			if signal == syscall.SIGUSR1 {
				Start()
			} else if signal == syscall.SIGHUP || signal == syscall.SIGUSR2 {
				Stop()
			} else {
				Stop()
				os.Exit(0)
			}
		}
	}()

	registerTraceDBDriver()
}

func registerTraceDBDriver() {
	regexCutSpace := regexp.MustCompile(`[ \r\n\t]{1,}`)
	regexTagComment := regexp.MustCompile(`(/\* *(.*?) *\*/)`)

	PreFunc := func(c context.Context, stmt *proxy.Stmt, args []driver.NamedValue) (interface{}, error) {
		return time.Now().UnixNano(), nil
	}
	PostFunc := func(c context.Context, ctx interface{}, stmt *proxy.Stmt, args []driver.NamedValue, err error) error {
		if sqlLogFile != nil && err != driver.ErrSkip {
			now := time.Now()
			startTime := ctx.(int64)
			timeDelta := now.UnixNano() - startTime
			query := regexCutSpace.ReplaceAllString(stmt.QueryString, " ")
			posList := regexTagComment.FindStringSubmatchIndex(query)
			tag := ""
			if posList != nil {
				tag = query[posList[4]:posList[5]]
				query = query[:posList[1]]
			}
			fmt.Fprintf(sqlLogFile, "%d\t%d\t%s\t%s\n", startTime, timeDelta, tag, query)
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

// Start ISUCON Tracer Start
func Start() {

	var err error

	if TraceID != "" {
		Stop()
	}

	const tmpDirName = "/tmp"

	TraceID = time.Now().Format("20060102-150405")
	log.Printf("ISUCON Tracer Start (%s)\n", TraceID)

	// Start Profiler
	profilerHandle = profile.Start(profile.ProfilePath(tmpDirName), profile.NoShutdownHook)

	// Create SQL Log File
	sqlLogFileName = path.Join(tmpDirName, "sql.log")
	if sqlLogFile, err = os.Create(sqlLogFileName); err != nil {
		log.Printf("ISUCON Tracer Error: %s\n", err.Error())
		return
	}

	// Create Perfomance Log File
	perfomanceLogFileName = path.Join(tmpDirName, "perf.log")
	if perfomanceLogFile, err = os.Create(perfomanceLogFileName); err != nil {
		log.Printf("ISUCON Tracer Error: %s\n", err.Error())
		return
	}

	// Create Webroute Log File
	webrouteLogFileName = path.Join(tmpDirName, "webroute.log")
	if webrouteLogFile, err = os.Create(webrouteLogFileName); err != nil {
		log.Printf("ISUCON Tracer Error: %s\n", err.Error())
		return
	}
}

// Stop ISUCON Tracer Stop
func Stop() {
	if TraceID != "" {
		log.Printf("ISUCON Tracer End (%s)\n", TraceID)
		TraceID = ""
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
