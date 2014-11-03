package gzip

import (
    "fmt"
    "time"
    "github.com/mozilla-services/heka/message"
    . "github.com/mozilla-services/heka/pipeline"
    "bytes"
    "compress/gzip"
    "code.google.com/p/go-uuid/uuid"
    "sync"
)

type GzipMultiFilter struct {
    *GzipMultiFilterConfig
    batchChan    chan []byte
    tagChan      chan string
    msgLoopCount uint
}

type GzipMultiFilterConfig struct {
    FlushInterval uint32 `toml:"flush_interval"`
    FlushBytes    int    `toml:"flush_bytes"`
    GzipTag       string `toml:"gzip_tag"`
    EncoderName   string `toml:"encoder"`
    FieldTag      string `toml:"field_tag"`
}

func (f *GzipMultiFilter) ConfigStruct() interface{} {
    return &GzipMultiFilterConfig{
    FlushInterval: 1000,
    FlushBytes:    10,
    GzipTag:       "compressed",
    }
}

func (f *GzipMultiFilter) Init(config interface{}) (err error) {
    f.GzipMultiFilterConfig = config.(*GzipMultiFilterConfig)
    f.batchChan = make(chan []byte)
    f.tagChan = make(chan string, 1)

    if f.GzipTag == "" {
        return fmt.Errorf(`A gzip_tag value must be specified for the GzipTag Field`)
    }

    if f.FieldTag == "" {
        return fmt.Errorf(`A field_tag value must be specified`)
    }

    if f.EncoderName == "" {
        return fmt.Errorf(`An encoder must be specified`)
    }

    return
}

func (f *GzipMultiFilter) committer(fr FilterRunner, h PluginHelper, wg *sync.WaitGroup) {

    var (
        tag string
        outBatch []byte
    )

    tag = f.GzipTag

    for outBatch = range f.batchChan {
        pack := h.PipelinePack(f.msgLoopCount)
        if pack == nil {
            fr.LogError(fmt.Errorf("exceeded MaxMsgLoops = %d",
                h.PipelineConfig().Globals.MaxMsgLoops))
            break   
        }
        
        tagField, _ := message.NewField("GzipTag", tag, "")
        pack.Message.AddField(tagField)
        tagField, _ = message.NewField("Partition", <- f.tagChan, "")
        pack.Message.AddField(tagField)
        pack.Message.SetUuid(uuid.NewRandom())
        pack.Message.SetPayload(string(outBatch[:]))
        fr.Inject(pack)

    }
    wg.Done()
}

func (f *GzipMultiFilter) receiver(fr FilterRunner, h PluginHelper, encoder Encoder, wg *sync.WaitGroup) {
    var (
        pack      *PipelinePack
        ok        bool   
        e         error
        partition string
    )
    ok = true
    batches  := make(map[string]*bytes.Buffer)
    writers  := make(map[string]*gzip.Writer)
    outBytes := make([]byte, 0, 10000)
    ticker   := time.Tick(time.Duration(f.FlushInterval) * time.Millisecond)
    inChan   := fr.InChan()

    for ok {
        select {  
            case pack, ok = <-inChan:
                if !ok {
                    // Closed inChan => we're shutting down, flush data
                    for _, value := range batches {
                        if value.Len() > 0 {
                            f.batchChan <- value.Bytes()
                        }
                    }
                    close(f.batchChan)
                    break
                } 
                f.msgLoopCount = pack.MsgLoopCount

                if outBytes, e = encoder.Encode(pack); e != nil {
                    fr.LogError(fmt.Errorf("Error encoding message: %s", e))
                } else {
                    if len(outBytes) > 0 {
                        partition = pack.Message.FindFirstField(f.FieldTag).ValueString[0]
                        _, ohk := batches[partition]
                        if !ohk { 
                            batches[partition] = new(bytes.Buffer)
                            writers[partition] = gzip.NewWriter(batches[partition])
                        } 

                        writers[partition].Write(outBytes)

                        if batches[partition].Len() > f.FlushBytes { 
                            writers[partition].Close()
                            f.tagChan <- partition
                            f.batchChan <- batches[partition].Bytes()
                            batches[partition].Reset()
                            writers[partition] = gzip.NewWriter(batches[partition])
                        }
                    }
                    outBytes = outBytes[:0]
                } 
                pack.Recycle()

            case <- ticker:
                for key, value := range batches {
                    if value.Len() > 0 {
                        writers[key].Close()
                        f.tagChan <- key
                        f.batchChan <- value.Bytes()
                        delete(batches, key)
                        delete(writers, key)
                    }
                } 
        }
    }

    wg.Done()
}

func (f *GzipMultiFilter) Run(fr FilterRunner, h PluginHelper) (err error) {
    base_name := f.EncoderName
    full_name := fr.Name() + "-" + f.EncoderName
    encoder, ok := h.Encoder(base_name, full_name)
    if !ok {
        return fmt.Errorf("Encoder not found: %s", full_name)
    }

    var wg sync.WaitGroup
    wg.Add(2)
    go f.receiver(fr, h, encoder, &wg)
    go f.committer(fr, h, &wg)
    wg.Wait()

    return
}

func init() {
    RegisterPlugin("GzipMultiFilter", func() interface{} {
        return new(GzipMultiFilter)
    })
}


