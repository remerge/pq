package pq

// TODO - proper error handling!

import (
	"bytes"
	"database/sql/driver"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"time"
)

type XLogData struct {
	// Type  byte
	Start int64
	End   int64
	Clock int64
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

type OldKeys struct {
	Names  []string      `json:"keynames,omitempty"`
	Types  []string      `json:"keytypes,omitempty"`
	Values []interface{} `json:"keyvalues,omitempty"`
}

type Change struct {
	Kind string `json:"kind,omitempty"`
	// Schema  string        `json:"schema,omitempty"`
	Table   string        `json:"table,omitempty"`
	Columns []string      `json:"columnnames,omitempty"`
	Values  []interface{} `json:"columnvalues,omitempty"`
	OldKeys OldKeys       `json:"oldkeys,omitempty"`
}
type ChangeSet struct {
	Xid     int32    `json:"xid,omitempty"`
	Changes []Change `json:"change,omitempty"`
	LogPos  int64
	confirm chan int64
}

func WAL(i int64) string {
	return fmt.Sprintf("%X/%X", uint32(i>>32), uint32(i))
}

func (cn *conn) SimpleQuery(q string) (res driver.Rows, err error) {
	return cn.simpleQuery(q)
}

// https://github.com/postgres/postgres/blob/master/src/bin/pg_basebackup/streamutil.c#L236-L238
func GetCurrentTimestamp() int64 {
	t := time.Now().UnixNano() / 1000
	return t - (((2451545 - 2440588) * 86400) * 1000000)
}

func (cn *conn) feedback(lsn int64) {
	response := StatusResponse{
		H: ResponseHeader{
			Type: 'd',
			Len:  1 + 8 + 8 + 8 + 8 + 1 + 4, // the last + 4 ist for the length itself -> pg convention
		},
		Type:  'r',
		Time:  GetCurrentTimestamp(),
		Write: lsn,
		Flush: lsn,
	}

	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.BigEndian, &response)

	// fmt.Println("time", response.Time, "write", WAL(response.Write), "type", string(response.Type), "err", err)

	_, err = cn.c.Write(buf.Bytes())

	// fmt.Println("written", n, "err", err)
	if err != nil {
		panic(err)
	}
}

func (cs *ChangeSet) Confirm() {
	cs.confirm <- cs.LogPos

}

func (cn *conn) StartReplicationStream(slot string, wal int64) (msgs chan *ChangeSet, err error) {
	hi := uint32(wal >> 32)
	lo := uint32(wal)
	query := fmt.Sprintf("START_REPLICATION SLOT %s LOGICAL %X/%X", slot, hi, lo)
	// fmt.Println("rep cmd:", query)
	return cn.StreamQuery(query, wal)
}

func (cn *conn) StreamQuery(q string, wal int64) (msgs chan *ChangeSet, err error) {

	defer cn.errRecover(&err)

	msgs = make(chan *ChangeSet)
	confirm := make(chan int64)

	b := cn.writeBuf('Q')
	b.string(q)
	cn.send(b)

	t, r := cn.recv1()
	// oh no :(
	if t == 'E' {
		panic(parseError(r))
	}

	// CopyBothResponse message
	if t != 'W' {
		panic("expected CopyBothResponse")
	}

	// now we are in streaming mode
	// current lsn
	var lsn int64 = -1

	// confirm channel
	go func() {
		for {
			last := lsn
			lsn = <-confirm
			if lsn > last {
				// fmt.Println("confirming ", WAL(lsn))
				cn.feedback(lsn)
			}
		}
	}()

	if wal > 0 {
		confirm <- wal
	}

	// keep alive ticker
	ticker := time.NewTicker(5 * time.Second)

	// terminate later
	quit := make(chan struct{})
	go func() {
		for {
			select {
			case <-ticker.C:
				// fmt.Println("send keepalive")
				cn.feedback(lsn)
			case <-quit:
				ticker.Stop()
				return
			}
		}
	}()

	// main receiver
	go func() {
		buffer := bytes.Buffer{}
		discard := false
		var last, secondLast byte
		var level int
		// var start *time.Time
		for {
			t, r := cn.recv1()
			t = r.byte()
			// fmt.Println("recv1", string(t), t, string(*r))

			inString := false
			switch t {
			case 'k':
				var serverWAL, time int64
				var reply byte

				// fmt.Println("keepalive", len(*r))
				buf := bytes.NewReader(*r)
				binary.Read(buf, binary.BigEndian, &serverWAL)
				binary.Read(buf, binary.BigEndian, &time)
				binary.Read(buf, binary.BigEndian, &reply)

				// fmt.Println("serverWal", WAL(serverWAL), "time", time, "reply", reply)

				if lsn == -1 {
					lsn = 0
				}
				// if serverWAL > lsn {
				// 	// TODO - this is wrong - we can't confim until it is actually transacted away
				// 	lsn = serverWAL
				// }

				if reply > 0 {
					confirm <- lsn
				}
			case 'w':
				buf := bytes.NewReader(*r)
				header := &XLogData{}
				binary.Read(buf, binary.BigEndian, header)
				// if start == nil {
				// 	t := time.Now()
				// 	start = &t
				// }

				// msg should always start with a { if level is 0
				if level == 0 && (*r)[24:][0] != '{' {
					fmt.Printf("WARNING level is 0 but msg does not start with a { - TODO discard. msg->\n%q\n----------\n", (*r)[24:])
					// TODO - set discard to true later
				}

				if !discard {
					// scan for not escape open and close pairs
					for _, c := range (*r)[24:] {
						if !inString {
							if c == '{' {
								level++
							}
							if c == '}' {
								level--
							}
						}
						if !(secondLast == '\\' && last == '\\') && last != '\\' && c == '"' {
							inString = !inString
						}
						secondLast = last
						last = c
					}

					buffer.Write((*r)[24:])
				}

				if !discard && buffer.Len() > 1000000 {
					discard = true
					fmt.Println("WARNING - huge changeset detected, skipping for now!")
				}

				fmt.Printf("----- chunk start wal_start=%v wal_end=%v level_after_msg=%d discard=%t------\n", WAL(header.Start), WAL(header.End), level, discard)
				fmt.Printf("%q\n", string((*r)[24:]))
				fmt.Printf("----- chunk end c-1=%q c-2=%q --------\n", (*r)[len(*r)-1], (*r)[len(*r)-2])

				s := (*r)[len(*r)-2]
				if !((*r)[len(*r)-1] == '}' && (s == '\n' || s == ']')) {
					continue
				}

				if discard {
					buffer.Reset()
					discard = false
					confirm <- header.Start
					last = 0
					level = 0
				} else {
					// this is some partial json
					set := &ChangeSet{}
					// in theory we only need to call decode if level is zero
					// kind of ugly but there is no other way - reusing the decoder does not work
					dec := json.NewDecoder(bytes.NewReader(buffer.Bytes()))
					// TODO this needs more error handling so we dont get stuck in the middle!
					if err := dec.Decode(&set); err == io.EOF {
						// fmt.Println("eof")
					} else if err != nil {
						fmt.Println("wait for more data", err)
					} else {
						fmt.Printf("msg properly decoded len=%d level=%d\n", buffer.Len(), level)
						// start = nil
						// OK case
						buffer.Reset()
						last = 0
						// dec.Buffered().Read(buffer.Bytes())
						// fmt.Println("got messages", string(buffer.Bytes()))
						// fmt.Println("set ok")
						set.LogPos = header.Start
						set.confirm = confirm
						msgs <- set
					}
				}

			}
		}
	}()
	return msgs, err
}
