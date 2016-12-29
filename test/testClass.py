class MyIterator(object):
        def __init__(self, step):
            print("_init_%d"%step)
            self.step = step
        def next(self):
            """Returns the next element."""
            if self.step == 0:
                raise StopIteration
            self.step -= 1
            print("_next_%d"%self.step)
            return self.step
        def __iter__(self):
            """Returns the iterator itself."""
            print("_iter_%d"%self.step)
            return self


for el in MyIterator(4):print el