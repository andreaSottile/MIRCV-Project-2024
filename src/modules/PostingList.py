class PostingList:
    def __init__(self, key):
        self.key = key  # token, string
        self.current = 0  # pointer to current index, int
        self.docids = []  # list of doc ids
        self.freqs = []  # list of frequencies
        self.size = -1  # len(docids) = len(freqs)

    # functions: described in the laboratory slides
    def set_docids(self, d_list):
        self.docids = d_list
        self.size = min(len(self.docids), len(self.freqs))

    def set_freqs(self, f_list):
        for f in f_list:
            self.freqs.append(int(f))
        self.size = min(len(self.docids), len(self.freqs))

    def key(self):
        return self.key

    def token(self):
        return self.key

    def docid(self):
        return self.docids[self.current]

    def freq(self):
        return self.freqs[self.current]

    def next(self):
        self.current += 1

    def nextGEQ_posting(self, d):
        # Theory
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

    def move_cursor(self, p):
        self.current = p
