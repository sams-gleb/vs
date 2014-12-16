import os
import re
import commands

class VideoInspector(object):

    _valid = False

    def __init__(self, video_source, ffmpeg_bin="ionice -c2 ffmpeg"):
        if not os.path.exists(video_source):
            raise Exception('InputFileDoesNotExist')

        self.filename = os.path.basename(video_source)
        self.path = os.path.dirname(video_source)
        self.full_filename = video_source

        self._exec_response = commands.getoutput("%s -i %s -codec copy -f rawvideo -y /dev/null" % (
            ffmpeg_bin,
            self.full_filename
        ))


        if re.search(
            ".*command\snot\sfound",
            self._exec_response,
            flags=re.IGNORECASE
        ):
            raise Exception('CommandError')
        if re.search(
            "Duration: N\/A",
            self._exec_response,
            flags=re.IGNORECASE | re.MULTILINE
        ):
            raise Exception('UnreadableFile')
        if re.search(
            ".*invalid\sdata\sfound",
            self._exec_response,
            flags=re.IGNORECASE
        ):
            raise Exception('UnreadableFile')
        if re.search(
            ".*does\snot\scontain\sany\sstream",
            self._exec_response,
            flags=re.IGNORECASE
        ):
            raise Exception('UnreadableFile')


        self._valid = True

    def duration(self):
        if not self._valid:
            return
        units = self.raw_duration().split(":")
        return (int(units[0]) * 60 * 60 * 1000) + \
            (int(units[1]) * 60 * 1000) + \
            int(float(units[2]) * 1000)

    def fps(self):
        if not self._valid:
            return
        return re.search("([0-9\.]+) (fps|tb)", self._exec_response).group(1)

    def frame(self):
        if not self._valid:
            return
        return re.search("(frame=)\s+([0-9]+)", self._exec_response).group(2)

    def encode(self):
	if not self._valid:
            return
        return re.search("(encode)\s*:\s*([a-zA-Z0-9]+)", self._exec_response).group(2)

    def video_stream(self):
        m = re.search("\n\s*Stream.*Video:.*\n", self._exec_response)

        if m:
            return m.group(0).strip()
        return

    def _video_match(self):
        if not self._valid:
            return

        m = re.search(
            "Stream\s*(.*?)[,|:|\(|\[].*?\s*"
            "Video:\s*(.*?),\s*(.*?),\s*(\d*)x(\d*)",
            self.video_stream()
        )

        if not m:
            m = re.search(
                "Stream\s*(.*?)[,|:|\(|\[].*?\s*Video:\s*(.*?),\s*(\d*)x(\d*)",
                self.video_stream()
            )

        return m

    def video_stream_id(self):
        if not self._valid:
            return
        return self._video_match().group(1)

    def video_codec(self):
        if not self._valid:
            return
        return self._video_match().group(2)

    def audio_stream(self):
        if not self._valid:
            return
        m = re.search("\n\s*Stream.*Audio:.*\n", self._exec_response)
        if m:
            return m.group(0).strip()
        return

    def _audio_match(self):
        if not self._valid:
            return
        return re.search(
            "Stream\s*(.*?)[,|:|\(|\[].*?\s*Audio:\s*(.*?),\s*([0-9\.]*) "
            "(\w*),\s*([a-zA-Z:]*)",
            self.audio_stream()
        )

    def audio_codec(self):
        if not self._valid:
            return
        return self._audio_match().group(2)


