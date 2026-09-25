package store

import (
	"bytes"
	"errors"
	"fmt"
	"sync"
	"testing"
)

// TestConcurrentWritersDeterministic 多写入者以各自 (version, writer) 并发
// 覆盖同一对象。可见性规则是确定性全序：最高 version 取胜；
// 因此无论调度如何，最终内容必须固定为 vHigh 的内容，且中间任何读取都
// 只能看到完整快照，读不到半条带。
func TestConcurrentWritersDeterministic(t *testing.T) {
	s, _ := newTestStore(t, 5, Config{BlockSize: 16, Shards: 4, Replicas: 2})
	obj := "counter"
	base := bytes.Repeat([]byte("0"), 32) // 2 个块
	if err := s.WriteObject(obj, base, WriteOptions{Version: 0, Writer: "init"}); err != nil {
		t.Fatal(err)
	}

	const writers = 8
	var wg sync.WaitGroup
	var readerWG sync.WaitGroup
	errCh := make(chan error, writers+1)
	stop := make(chan struct{})

	// 读者：在写入进行中反复读取，断言永远读不到"半成品"——
	// 所有快照长度均固定为 32 字节，readBlock 还会校验块 CRC；
	// 本例无故障，读取要么成功且长度正确，要么明确报错。
	readerWG.Add(1)
	go func() {
		defer readerWG.Done()
		for {
			select {
			case <-stop:
				return
			default:
				got, err := s.ReadObject(obj)
				if err != nil {
					errCh <- fmt.Errorf("reader unexpected error: %w", err)
					return
				}
				if len(got) != len(base) {
					errCh <- fmt.Errorf("reader saw torn data len=%d", len(got))
					return
				}
			}
		}
	}()

	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			tag := fmt.Sprintf("w%07d", w)       // 8 字节（w + 7 位数字）
			data := bytes.Repeat([]byte(tag), 4) // 32 字节 = 2 块
			err := s.WriteObject(obj, data, WriteOptions{Version: int64(w + 1), Writer: fmt.Sprintf("writer-%d", w)})
			if err != nil {
				errCh <- err
			}
		}(w)
	}
	wg.Wait()
	close(stop)
	readerWG.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatalf("并发期间出现非预期错误/撕裂读: %v", err)
	}

	got, err := s.ReadObject(obj)
	if err != nil {
		t.Fatalf("final read: %v", err)
	}
	// 确定性结论：version=8（writers 中最高）获胜。
	want := bytes.Repeat([]byte("w0000007"), 4)
	if !bytes.Equal(got, want) {
		t.Fatalf("并发结果不确定: got=%q want=%q", got, want)
	}
	t.Logf("并发写入后可见布局（确定性胜出者 v8/writer-7）:\n%s", s.DescribeLayout(obj))
	t.Logf("判定依据: 快照全序 (version, writer) 取最大，与 WAL 追加顺序/调度无关")
}

// TestSameVersionConcurrentDifferentWriters 同 version 不同 writer 并发：
// writer 字典序最大者确定性获胜，重复执行结论一致。
func TestSameVersionConcurrentDifferentWriters(t *testing.T) {
	for iter := 0; iter < 5; iter++ {
		s, _ := newTestStore(t, 4, Config{BlockSize: 64, Shards: 4, Replicas: 2})
		obj := "x"
		initData := make([]byte, 64)
		if err := s.WriteObject(obj, initData, WriteOptions{Version: 1, Writer: "0init"}); err != nil {
			t.Fatal(err)
		}
		var wg sync.WaitGroup
		for _, name := range []string{"alpha", "bravo", "charlie", "delta"} {
			n := name
			wg.Add(1)
			go func() {
				defer wg.Done()
				payload := bytes.Repeat([]byte(n[:1]), 64)
				if err := s.WriteObject(obj, payload, WriteOptions{Version: 2, Writer: n}); err != nil {
					t.Errorf("write %s: %v", n, err)
				}
			}()
		}
		wg.Wait()
		got, err := s.ReadObject(obj)
		if err != nil {
			t.Fatalf("iter %d read: %v", iter, err)
		}
		if got[0] != 'd' { // delta 字典序最大
			t.Fatalf("iter %d: 确定性胜出者应为 delta，实际首字节=%q", iter, got[0])
		}
	}
	t.Log("判定依据: 同 version 时 writer 字典序最大者(delta)胜出，5 次重复结论一致")
}

// TestConcurrentWritesAndGC 写入者持续产生新版本与旧版本垃圾的同时，
// GC 反复回收。任何时刻读取都必须成功且数据自洽（长度固定、块 CRC 有效）；
// 结束后占用必须与存活集合一致。
func TestConcurrentWritesAndGC(t *testing.T) {
	s, _ := newTestStore(t, 5, Config{BlockSize: 16, Shards: 4, Replicas: 2})
	obj := "gcrace"
	const size = 32
	if err := s.WriteObject(obj, bytes.Repeat([]byte("0"), size), WriteOptions{Version: 0, Writer: "init"}); err != nil {
		t.Fatal(err)
	}
	const rounds = 12
	var wg sync.WaitGroup
	var gcWG sync.WaitGroup
	stop := make(chan struct{})
	stopGC := make(chan struct{})
	errCh := make(chan error, 8)

	// 持续读者
	readerWG := new(sync.WaitGroup)
	readerWG.Add(1)
	go func() {
		defer readerWG.Done()
		for {
			select {
			case <-stop:
				return
			default:
				got, err := s.ReadObject(obj)
				if err != nil {
					errCh <- fmt.Errorf("read during gc: %w", err)
					return
				}
				if len(got) != size {
					errCh <- fmt.Errorf("torn read len=%d", len(got))
					return
				}
			}
		}
	}()

	// 写入者
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for v := 1; v <= rounds; v++ {
				tag := []byte(fmt.Sprintf("w%dv%05d", w, v))
				data := make([]byte, size)
				for i := range data {
					data[i] = tag[i%len(tag)]
				}
				if err := s.WriteObject(obj, data, WriteOptions{Version: int64(v*10 + w), Writer: fmt.Sprintf("w%d", w)}); err != nil {
					errCh <- err
					return
				}
			}
		}(w)
	}

	// GC 循环：写入全部结束后由主流程通知停止。
	gcWG.Add(1)
	go func() {
		defer gcWG.Done()
		for {
			select {
			case <-stopGC:
				return
			default:
				if _, err := s.RunGC(); err != nil {
					errCh <- err
					return
				}
			}
		}
	}()

	wg.Wait()
	close(stopGC)
	gcWG.Wait()
	close(stop)
	readerWG.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}
	// 最终再来一次确定性 GC 并核对占用。
	rep, err := s.RunGC()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.ReadObject(obj); err != nil {
		t.Fatalf("final read: %v", err)
	}
	usage, _ := s.SpaceUsage()
	live := s.liveShardFiles()
	var want int64
	for id, ps := range live {
		for p := range ps {
			want += s.Backend(id).SizeBytes(p)
		}
	}
	var have int64
	for _, n := range usage {
		have += n
	}
	if have != want {
		t.Fatalf("并发 GC 后占用 %d != 存活 %d", have, want)
	}
	t.Logf("并发写入+GC 结束，最终 GC 删除=%d，占用与存活一致 (%d 字节)", len(rep.DeletedFiles), have)
}

// TestConcurrentSameKeyConflictingPayloads 多个 goroutine 用完全相同的
// (version, writer) 提交不同载荷。语义：除先声明者外都必须在写盘前收到
// ErrConflict；最终可见数据必须恰好等于某一个完整载荷，绝不能因互相
// 覆盖同一版本路径而产生损坏/读失败。
func TestConcurrentSameKeyConflictingPayloads(t *testing.T) {
	s, _ := newTestStore(t, 4, Config{BlockSize: 16, Shards: 4, Replicas: 2})
	obj := "samekey"
	// 先建立同键的基准提交，后续同键不同载荷应全部冲突。
	base := bytes.Repeat([]byte("B"), 16)
	if err := s.WriteObject(obj, base, WriteOptions{Version: 1, Writer: "w"}); err != nil {
		t.Fatal(err)
	}
	const n = 12
	payloads := make(map[string]bool)
	for i := 0; i < n; i++ {
		payloads[string(bytes.Repeat([]byte{byte('A' + i)}, 16))] = true
	}
	var wg sync.WaitGroup
	var mu sync.Mutex
	conflicts, others := 0, 0
	for p := range payloads {
		payload := []byte(p)
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := s.WriteObject(obj, payload, WriteOptions{Version: 1, Writer: "w"})
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil:
			case errors.Is(err, ErrConflict):
				conflicts++
			default:
				others++
				t.Errorf("非预期错误: %v", err)
			}
		}()
	}
	wg.Wait()
	if conflicts == 0 {
		t.Fatalf("同键不同载荷并发应至少产生一次 ErrConflict")
	}
	if others != 0 {
		t.Fatalf("出现非冲突类错误 %d 个", others)
	}
	got, err := s.ReadObject(obj)
	if err != nil {
		t.Fatalf("冲突后读取必须正常: %v", err)
	}
	if !payloads[string(got)] && string(got) != string(base) {
		t.Fatalf("最终数据既非基准也非任一完整载荷（可能被污染）: %q", got)
	}
	// 由于基准已先提交，先声明规则下所有新载荷都应冲突，最终仍是基准。
	if string(got) != string(base) {
		t.Fatalf("已提交基准存在时，同键冲突载荷不应改变可见数据: got=%q", got)
	}
	t.Logf("判定: %d/%d 个冲突载荷在写盘前被 ErrConflict 拒绝，可见数据保持基准不变", conflicts, n)
}
