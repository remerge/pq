package pq

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"time"
)

// just for some temp debugging - remove later (if still here after 06/2018 just delete)
var DEBUG_STREAM_TRACE = false

type XLogDataHeader struct {
	Start uint64
	End   uint64
	Clock uint64
}

type XLogDataMsg struct {
	Header  XLogDataHeader
	Data    []byte
	confirm chan uint64
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

func (msg *XLogDataMsg) Confirm() {
	msg.confirm <- msg.Header.Start
}

func TRACE(format string, a ...interface{}) {
	if DEBUG_STREAM_TRACE {
		fmt.Printf("[pq/stream] "+format+"\n", a...)
	}
}

func INFO(format string, a ...interface{}) {
	fmt.Printf("[pq/stream] "+format+"\n", a...)
}

func ERROR(err error, format string, a ...interface{}) {
	if err == nil {
		return
	}
	errString := fmt.Sprintf(" err=%v", err)
	fmt.Printf("[pq/stream] ERROR: "+format+errString+"\n", a...)
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
	ERROR(err, "feedback message write failed")
	TRACE("feedback time=%v lsn=%v type=%v err=%v", response.Time, WAL(uint64(response.Write)), string(response.Type), err)

	n, err := cn.c.Write(buf.Bytes())
	ERROR(err, "feedback message write failed")
	TRACE("feedback written n=%v err=%v", n, err)

	if err != nil {
		panic(err)
	}
}

func (cn *conn) StartReplicationStream(slot string, wal uint64, quit chan struct{}) (msgs chan *XLogDataMsg, err error) {
	hi := uint32(wal >> 32)
	lo := uint32(wal)
	query := fmt.Sprintf("START_REPLICATION SLOT %s LOGICAL %X/%X", slot, hi, lo)
	TRACE("start replication query=%v", query)
	return cn.StreamQuery(query, quit)
}

func (cn *conn) StreamQuery(q string, quit chan struct{}) (msgs chan *XLogDataMsg, err error) {
	defer cn.errRecover(&err)

	msgs = make(chan *XLogDataMsg)
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
			if lsn >= last {
				TRACE("confirm lsn=%v last=%v", WAL(lsn), WAL(last))
				cn.feedback(lsn)
			}
			confirmed <- lsn
		}
	}()

	// keep alive ticker
	ticker := time.NewTicker(5 * time.Second)

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
		var lastConfirmedLsn uint64
		for {
			ch := make(chan *readBuf)
			go func() {
				defer cn.errRecover(&err)
				_, r := cn.recv1()
				ch <- r
			}()
			select {
			case <-quit:
				return
			case r := <-ch:
				t = r.byte()

				switch t {
				case 'k':
					var serverWAL, time uint64
					var reply byte

					buf := bytes.NewReader(*r)
					err := binary.Read(buf, binary.BigEndian, &serverWAL)
					ERROR(err, "keepalive read failed")
					err = binary.Read(buf, binary.BigEndian, &time)
					ERROR(err, "keepalive read failed")
					err = binary.Read(buf, binary.BigEndian, &reply)
					ERROR(err, "keepalive read failed")

					TRACE("keepalive server_lsn=%v time=%v reply=%v", WAL(serverWAL), time, reply)

					// 1 means that the client should reply to this message as soon as possible, to avoid a timeout disconnect. 0 otherwise.
					if reply == 1 && lastConfirmedLsn != 0 {
						INFO("keepalive server_lsn=%v local_lsn=%v time=%v reply=%v (timeout soon)", WAL(serverWAL), WAL(lsn), time, reply)
						// just resend the last lsn
						confirm <- lastConfirmedLsn
						<-confirmed
					}
				case 'w':
					var msg XLogDataMsg

					buf := bytes.NewReader(*r)
					err := binary.Read(buf, binary.BigEndian, &(msg.Header))
					ERROR(err, "message read failed")

					msg.Data = []byte((*r)[24:])
					msg.confirm = make(chan uint64)

					TRACE("recv msg header.Start=%v header.End=%v header.Clock=%v len=%v", WAL(msg.Header.Start), WAL(msg.Header.End), msg.Header.Clock, len(msg.Data))

					// wait for confirmation
					msgs <- &msg
					confirm <- <-msg.confirm
					lastConfirmedLsn = <-confirmed
				}
			}
		}
	}()

	return msgs, err
}
