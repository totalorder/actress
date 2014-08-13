# encoding: utf-8
import signal
def signal_handler(signal, frame):
    print "Sending the order..."
    print "All workers on death row"
signal.signal(signal.SIGINT, signal_handler)
print('Press Ctrl+C')
signal.pause()