import unittest

from video_study_translate_mvp import (
    SrtCue,
    adapt_text_for_timing,
    chunk_cues,
    classify_voice_gender_from_zero_crossing_rate,
    estimate_speech_rate_wpm,
    parse_srt,
    render_srt,
    resolve_tts_voice,
)


class VideoStudyTranslateMvpTests(unittest.TestCase):
    def test_parse_srt_reads_all_cues(self) -> None:
        raw = """
1
00:00:00,000 --> 00:00:02,000
Hello world

2
00:00:02,100 --> 00:00:04,500
Second line
with break
""".strip()

        cues = parse_srt(raw)

        self.assertEqual(len(cues), 2)
        self.assertEqual(
            cues[0],
            SrtCue(
                index=1,
                start="00:00:00,000",
                end="00:00:02,000",
                text="Hello world",
            ),
        )
        self.assertEqual(
            cues[1],
            SrtCue(
                index=2,
                start="00:00:02,100",
                end="00:00:04,500",
                text="Second line\nwith break",
            ),
        )

    def test_render_srt_roundtrip(self) -> None:
        cues = [
            SrtCue(index=1, start="00:00:00,000", end="00:00:01,000", text="A"),
            SrtCue(index=2, start="00:00:01,050", end="00:00:02,000", text="B"),
        ]

        rendered = render_srt(cues)
        reparsed = parse_srt(rendered)

        self.assertEqual(reparsed, cues)

    def test_chunk_cues_respects_limits(self) -> None:
        cues = [
            SrtCue(index=1, start="00:00:00,000", end="00:00:01,000", text="a" * 10),
            SrtCue(index=2, start="00:00:01,000", end="00:00:02,000", text="b" * 10),
            SrtCue(index=3, start="00:00:02,000", end="00:00:03,000", text="c" * 10),
            SrtCue(index=4, start="00:00:03,000", end="00:00:04,000", text="d" * 10),
        ]

        chunks = chunk_cues(cues, max_items=2, max_chars=25)

        self.assertEqual(len(chunks), 2)
        self.assertEqual([cue.index for cue in chunks[0]], [1, 2])
        self.assertEqual([cue.index for cue in chunks[1]], [3, 4])

    def test_estimate_speech_rate_wpm_returns_value(self) -> None:
        cues = [
            SrtCue(index=1, start="00:00:00,000", end="00:00:05,000", text="hello world"),
            SrtCue(index=2, start="00:00:05,000", end="00:00:10,000", text="this is a longer sentence"),
        ]

        wpm = estimate_speech_rate_wpm(cues)

        self.assertIsNotNone(wpm)
        self.assertGreaterEqual(wpm or 0.0, 90.0)
        self.assertLessEqual(wpm or 1000.0, 190.0)

    def test_classify_voice_gender_from_zero_crossing_rate(self) -> None:
        female_gender, female_confidence = classify_voice_gender_from_zero_crossing_rate(0.12)
        male_gender, male_confidence = classify_voice_gender_from_zero_crossing_rate(0.07)

        self.assertEqual(female_gender, "female")
        self.assertEqual(male_gender, "male")
        self.assertGreaterEqual(female_confidence, 0.0)
        self.assertGreaterEqual(male_confidence, 0.0)

    def test_adapt_text_for_timing_trims_word_count(self) -> None:
        text = "um dois tres quatro cinco seis sete oito nove dez onze doze treze"

        adapted = adapt_text_for_timing(
            text=text,
            target_duration_seconds=2.0,
            max_chars_per_cue=500,
        )

        self.assertLessEqual(len(adapted.split(" ")), 6)

    def test_resolve_tts_voice_auto_without_video_uses_male_fallback(self) -> None:
        voice, detected_gender, confidence = resolve_tts_voice(
            requested_voice="auto",
            input_video=None,
            timeout_seconds=30,
            male_voice="ash",
            female_voice="coral",
        )

        self.assertEqual(voice, "ash")
        self.assertIsNone(detected_gender)
        self.assertIsNone(confidence)


if __name__ == "__main__":
    unittest.main()
