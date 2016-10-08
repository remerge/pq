package pq

import (
	"bytes"
	"database/sql/driver"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"time"
)

type XLogData struct {
	Start uint64
	End   uint64
	Clock uint64
}

type ResponseHeader struct {
	Type byte
	Len  int32
}

type StatusResponse struct {
	H              ResponseHeader
	Type           byte
	Write          int64
	Flush          int64
	Apply          int64
	Time           int64
	ReplyRequested byte
}

type ChangeSet struct {
	Header  XLogData
	Msg     []byte
	confirm chan uint64
}

func (cs *ChangeSet) Confirm() {
	cs.confirm <- cs.Header.Start
}

func TRACE(format string, a ...interface{}) {
	if os.Getenv("PQ_TRACE") == "1" {
		fmt.Printf("[pq/stream] "+format+"\n", a...)
	}
}

func WAL(i uint64) string {
	return fmt.Sprintf("%X/%X", uint32(i>>32), uint32(i))
}

// https://github.com/postgres/postgres/blob/master/src/bin/pg_basebackup/streamutil.c#L236-L238
func GetCurrentTimestamp() int64 {
	t := time.Now().UnixNano() / 1000
	return t - (((2451545 - 2440588) * 86400) * 1000000)
}

func (cn *conn) feedback(lsn uint64) {
	response := StatusResponse{
		H: ResponseHeader{
			Type: 'd',
			Len:  1 + 8 + 8 + 8 + 8 + 1 + 4, // the last + 4 ist for the length itself -> pg convention
		},
		Type:  'r',
		Time:  GetCurrentTimestamp(),
		Write: int64(lsn),
		Flush: int64(lsn),
	}

	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.BigEndian, &response)

	TRACE("feedback time=%v lsn=%v type=%v err=%v", response.Time, WAL(uint64(response.Write)), string(response.Type), err)

	n, err := cn.c.Write(buf.Bytes())

	TRACE("feedback written n=%v err=%v", n, err)

	if err != nil {
		panic(err)
	}
}

func (cn *conn) StartReplicationStream(slot string, wal uint64) (msgs chan *ChangeSet, err error) {
	hi := uint32(wal >> 32)
	lo := uint32(wal)
	query := fmt.Sprintf("START_REPLICATION SLOT %s LOGICAL %X/%X", slot, hi, lo)
	TRACE("start replication query=%v", query)
	return cn.StreamQuery(query)
}

func (cn *conn) SimpleQuery(q string) (res driver.Rows, err error) {
	return cn.simpleQuery(q)
}

func (cn *conn) StreamQuery(q string) (msgs chan *ChangeSet, err error) {
	defer cn.errRecover(&err)

	msgs = make(chan *ChangeSet)
	confirm := make(chan uint64)
	confirmed := make(chan uint64)

	b := cn.writeBuf('Q')
	b.string(q)
	cn.send(b)

	t, r := cn.recv1()

	if t == 'E' {
		return nil, parseError(r)
	}

	if t != 'W' {
		return nil, errors.New("expected CopyBothResponse")
	}

	// now we are in streaming mode

	// current lsn
	var lsn uint64 = 0

	// confirm channel
	go func() {
		for {
			last := lsn
			lsn = <-confirm
			if lsn > last {
				TRACE("confirm lsn=%v last=%v", WAL(lsn), WAL(last))
				cn.feedback(lsn)
			}
			confirmed <- lsn
		}
	}()

	// keep alive ticker
	ticker := time.NewTicker(5 * time.Second)

	// terminate later
	quit := make(chan struct{})
	go func() {
		for {
			select {
			case <-ticker.C:
				TRACE("send keepalive lsn=%v", WAL(lsn))
				cn.feedback(lsn)
			case <-quit:
				ticker.Stop()
				return
			}
		}
	}()

	// main receiver
	go func() {
		for {
			t, r := cn.recv1()
			t = r.byte()

			switch t {
			case 'k':
				var serverWAL, time uint64
				var reply byte

				buf := bytes.NewReader(*r)
				binary.Read(buf, binary.BigEndian, &serverWAL)
				binary.Read(buf, binary.BigEndian, &time)
				binary.Read(buf, binary.BigEndian, &reply)

				TRACE("keepalive server_lsn=%v time=%v reply=%v", WAL(serverWAL), time, reply)

				if reply > 0 {
					confirm <- lsn
				}
			case 'w':
				var cs ChangeSet

				buf := bytes.NewReader(*r)
				binary.Read(buf, binary.BigEndian, &(cs.Header))

				cs.Msg = []byte((*r)[24:])
				cs.confirm = make(chan uint64)

				msgs <- &cs

				TRACE("recv msg header.Start=%v header.End=%v header.Clock=%v len=%v", WAL(cs.Header.Start), WAL(cs.Header.End), cs.Header.Clock, len(cs.Msg))

				// wait for confirmation
				confirm <- <-cs.confirm
				<-confirmed
			}
		}
	}()

	return msgs, err
}
