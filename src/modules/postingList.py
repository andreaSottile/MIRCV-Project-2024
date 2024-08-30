class postingList:
    def __init__(self, key):
        self.key = key
        self.current = 0
        self.scores = []
        self.docids = []
        self.freqs = []
        self.size = -1

    def set_docids(self, d_list):
        self.docids = d_list
        self.size = min(len(self.docids), len(self.freqs))
        self.reset_scores()

    def set_freqs(self, f_list):
        self.freqs = f_list
        self.size = min(len(self.docids), len(self.freqs))
        self.reset_scores()

    def key(self):
        return self.key

    def token(self):
        return self.key

    def docid(self):
        return self.docids[self.current]

    def freq(self):
        return self.freqs[self.current]

    def score(self):
        return self.scores[self.current]

    def next(self):
        self.current += 1

    def set_score(self, s, pos=-1):
        if pos == -1:
            self.scores[self.current] = s
        else:
            self.scores[pos] = s

    def nextGEQ_posting(self, d):
        if d in self.docids:
            geq = self.docids.index(d)
            if geq >= self.current:
                self.current = geq + 1
            else:
                print("error on posting list: cannot find nextGEQ than " + str(d))
        else:
            geq = self.current
            while True:
                if self.docids[geq] == d:
                    self.current = geq
                    break
                else:
                    geq += 1
                    if geq >= self.size:
                        print("error on posting list: cannot find nextGEQ than " + str(d))
                        break

    def reset_scores(self):
        self.scores = [0] * self.size

    def move_cursor(self, p):
        self.current = p
