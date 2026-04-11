import wave
import struct
import math

# Generate a simple 1-second 440Hz sine wave
sample_rate = 44100
duration = 1.0
frequency = 440.0

with wave.open('test_tone.wav', 'w') as wav_file:
    wav_file.setnchannels(1)
    wav_file.setsampwidth(2)
    wav_file.setframerate(sample_rate)
    
    for i in range(int(sample_rate * duration)):
        value = int(32767.0 * math.sin(2.0 * math.pi * frequency * i / sample_rate))
        data = struct.pack('<h', value)
        wav_file.writeframesraw(data)
