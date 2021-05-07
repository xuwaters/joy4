package rtsp

import (
	"io"
	"strings"
	"time"

	"github.com/aler9/gortsplib"
	"github.com/aler9/gortsplib/pkg/rtpaac"
	"github.com/aler9/gortsplib/pkg/rtph264"
	"github.com/nareix/joy4/av"
	"github.com/nareix/joy4/av/avutil"
	"github.com/nareix/joy4/codec/aacparser"
	"github.com/nareix/joy4/codec/h264parser"
	"github.com/nareix/joy4/format/rtsp/h264"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
)

func Handler(h *avutil.RegisterHandler) {
	h.UrlDemuxer = func(uri string) (ok bool, demuxer av.DemuxCloser, err error) {
		if !strings.HasPrefix(uri, "rtsp://") {
			return
		}
		ok = true
		demuxer, err = DialRead(uri)
		return
	}
}

type Client struct {
	conn    *gortsplib.ClientConn
	packets chan av.Packet
	tracks  gortsplib.Tracks
	streams []Stream
	closed  bool
}

var _ av.DemuxCloser = (*Client)(nil)

func DialRead(uri string) (c *Client, err error) {
	conn, err := gortsplib.DialRead(uri)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to dial uri: %s", uri)
	}

	// parsing tracks
	client := &Client{
		conn:    conn,
		packets: make(chan av.Packet, 60),
		tracks:  conn.Tracks(),
	}

	err = client.initStreams()
	if err != nil {
		client.Close()
		return nil, err
	}

	ch := client.conn.ReadFrames(client.onRtpFrame)
	go func() {
		<-ch
		log.Info().Str("uri", uri).Msg("finished reading frames")
		close(client.packets)
	}()

	return client, nil
}

func (c *Client) Close() error {
	log.Info().Bool("closed", c.closed).Msg("closing rtsp client")
	if c.closed {
		return nil
	}
	c.closed = true
	err := c.conn.Close()
	if err != nil {
		log.Error().Err(err).Msg("error closing rtsp client")
		return errors.Wrap(err, "failed to close rtsp")
	}
	log.Info().Msg("closed rtsp client")
	return nil
}

func (c *Client) Streams() ([]av.CodecData, error) {
	if len(c.tracks) == 0 {
		return nil, errors.Errorf("no track")
	}
	codecs := []av.CodecData{}
	for _, stream := range c.streams {
		codecs = append(codecs, stream.CodecData())
	}
	return codecs, nil
}

func (c *Client) initStreams() error {
	if c.streams == nil {
		streams := make([]Stream, 0)
		for _, track := range c.tracks {
			stream, err := trackAsStream(track)
			if err != nil {
				return err
			}
			streams = append(streams, stream)
		}
		c.streams = streams
	}
	return nil
}

func (c *Client) ReadPacket() (av.Packet, error) {
	pkt, ok := <-c.packets
	if !ok {
		return pkt, io.EOF
	}
	return pkt, nil
}

var ErrMorePacketsNeeded = errors.New("more packets required")

type Stream interface {
	CodecData() av.CodecData
	OnRtpPayload(buf []byte) ([]av.Packet, error)
	Track() *gortsplib.Track
}

const ptsOffset = 0 * time.Second

type VideoStream struct {
	codecData   av.VideoCodecData
	track       *gortsplib.Track
	h264Decoder *rtph264.Decoder
	videoDTSEst *h264.DTSEstimator
	videoBuf    [][]byte
}

var _ Stream = (*VideoStream)(nil)

func NewVideoStream(track *gortsplib.Track) (*VideoStream, error) {
	if !track.IsH264() {
		return nil, errors.New("should be h264 track")
	}

	sps, pps, err := track.ExtractDataH264()
	if err != nil {
		return nil, errors.Wrap(err, "failed to extract h264")
	}
	codec, err := h264parser.NewCodecDataFromSPSAndPPS(sps, pps)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create h264 codec")
	}

	stream := &VideoStream{
		codecData:   codec,
		track:       track,
		h264Decoder: rtph264.NewDecoder(),
		videoDTSEst: h264.NewDTSEstimator(),
	}

	return stream, nil
}

func (s *VideoStream) CodecData() av.CodecData {
	return s.codecData
}

func (s *VideoStream) Track() *gortsplib.Track {
	return s.track
}

func (s *VideoStream) OnRtpPayload(buf []byte) ([]av.Packet, error) {

	nalus, pts, err := s.h264Decoder.Decode(buf)
	if err != nil {
		if errors.Is(err, rtph264.ErrMorePacketsNeeded) {
			return nil, ErrMorePacketsNeeded
		}
		return nil, errors.Wrapf(err, "error decoding h264 payload")
	}

	isKeyFrame := false
	for _, nalu := range nalus {
		typ := h264.NALUType(nalu[0] & 0x1F)
		switch typ {
		case h264.NALUTypeSPS, h264.NALUTypePPS, h264.NALUTypeAccessUnitDelimiter:
			continue

		case h264.NALUTypeIDR:
			isKeyFrame = true
		}

		s.videoBuf = append(s.videoBuf, nalu)
	}

	if len(s.videoBuf) == 0 {
		return nil, ErrMorePacketsNeeded
	}

	marker := (buf[1] >> 7 & 0x1) > 0
	if marker {
		data, err := h264.EncodeAVCC(s.videoBuf)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to encode AVCC payload")
		}

		dts := s.videoDTSEst.Feed(pts + ptsOffset)
		pkt := av.Packet{
			IsKeyFrame:      isKeyFrame,
			Idx:             int8(s.track.ID),
			CompositionTime: pts + ptsOffset - dts,
			Time:            dts,
			Data:            data,
		}
		s.videoBuf = s.videoBuf[:0]

		return []av.Packet{pkt}, nil
	}

	return nil, ErrMorePacketsNeeded
}

type AudioStream struct {
	codecData  av.AudioCodecData
	track      *gortsplib.Track
	aacDecoder *rtpaac.Decoder
	clockRate  int
}

var _ Stream = (*AudioStream)(nil)

func NewAudioStream(track *gortsplib.Track) (*AudioStream, error) {
	if !track.IsAAC() {
		return nil, errors.New("should be aac track")
	}
	aacConfig, err := track.ExtractDataAAC()
	if err != nil {
		return nil, err
	}
	codec, err := aacparser.NewCodecDataFromMPEG4AudioConfigBytes(aacConfig)
	if err != nil {
		return nil, err
	}
	clockRate, err := track.ClockRate()
	if err != nil {
		return nil, err
	}
	stream := &AudioStream{
		codecData:  codec,
		track:      track,
		aacDecoder: rtpaac.NewDecoder(clockRate),
		clockRate:  clockRate,
	}
	return stream, nil
}

func (s *AudioStream) CodecData() av.CodecData {
	return s.codecData
}

func (s *AudioStream) Track() *gortsplib.Track {
	return s.track
}

func (s *AudioStream) OnRtpPayload(buf []byte) ([]av.Packet, error) {
	aus, pts, err := s.aacDecoder.Decode(buf)
	if err != nil {
		if errors.Is(err, rtpaac.ErrMorePacketsNeeded) {
			return nil, ErrMorePacketsNeeded
		}
		return nil, errors.Wrapf(err, "failed to decode aac payload")
	}

	if len(aus) == 0 {
		return nil, ErrMorePacketsNeeded
	}

	pkts := make([]av.Packet, 0, len(aus))
	for i, au := range aus {
		auPTS := pts + ptsOffset + time.Duration(i)*1000*time.Second/time.Duration(s.clockRate)

		pkt := av.Packet{
			IsKeyFrame: false,
			Idx:        int8(s.track.ID),
			Data:       au,
			Time:       auPTS,
		}

		pkts = append(pkts, pkt)
	}

	return pkts, nil
}

func trackAsStream(track *gortsplib.Track) (Stream, error) {
	if track.IsH264() {
		return NewVideoStream(track)
	}
	if track.IsAAC() {
		return NewAudioStream(track)
	}
	return nil, errors.Errorf("invalid track")
}

func (c *Client) onRtpFrame(trackID int, streamType gortsplib.StreamType, buf []byte) {
	switch streamType {
	case gortsplib.StreamTypeRTCP:
		// ignore

	case gortsplib.StreamTypeRTP:
		if trackID >= 0 && trackID <= len(c.streams) {
			stream := c.streams[trackID]
			pkts, err := stream.OnRtpPayload(buf)
			if err != nil {
				if errors.Is(err, ErrMorePacketsNeeded) {
					return
				}
				log.Error().Err(err).Msg("failed to decode rtp payload")
				return
			}
			for _, pkt := range pkts {
				select {
				case c.packets <- pkt:
				default:
					// leaky
					<-c.packets
					c.packets <- pkt
				}
			}
		}
	}
}
