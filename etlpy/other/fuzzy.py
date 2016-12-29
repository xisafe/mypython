
def bitap_fuzzy_bitwise_search(text, pattern, k):
    if not pattern:
        return 0

    m = len(pattern)
    pattern_mask = {}
    for i in xrange(m):
        idx = pattern[i]
        if not pattern_mask.has_key(idx):
            pattern_mask[idx] = ~0
        pattern_mask[idx] &= ~(1<<i)

    result = -1
    R= [~1]*(k + 1)
    for i, c in enumerate(text):
        mask = pattern_mask.has_key(c) and pattern_mask[c] or ~0
        old_Rd1 = R[0]
        R[0] |= mask
        R[0] <<= 1
        for d in xrange(1, k+1): 
            tmp = R[d]
            R[d] = (old_Rd1 & (R[d] | mask)) << 1
            old_Rd1 = tmp

        if 0 == (R[k] & (1<<m)):
            result = i - m + 1
            break
    return result

class FuzzyBitapPattern:
    def __init__(self, pattern):
        self.pattern = pattern
        self.pattern_mask = {}
        for i, c in enumerate(self.pattern):
            if not self.pattern_mask.has_key(c):
                self.pattern_mask[c] = ~0
            self.pattern_mask[c] &= ~(1<<i)

    def pos(self, text, k=4):
        m = len(self.pattern)
        result = -1
        R= [~1]*(k + 1)
        for i, c in enumerate(text):
            mask = self.pattern_mask.get(c, ~0)
            old_Rd1 = R[0]
            R[0] |= mask
            R[0] <<= 1
            for d in xrange(1, k+1): 
                tmp = R[d]
                R[d] = (old_Rd1 & (R[d] | mask)) << 1
                old_Rd1 = tmp
            if 0 == (R[k] & (1<<m)):
                result = i - m + 1
                break
        return result

    def search(self, text, k=None, pad=" "):
        m = len(self.pattern)
        if k == None:
            k = m - 1
        if pad:
            padn = m - len(text)
            if padn > 0:
                text += pad[0]*padn 
        result = (-1, 0.0)
        R= [~1]*(k + 1)
        for i, c in enumerate(text):
            mask = self.pattern_mask.get(c, ~0)
            old_Rd1 = R[0]
            R[0] |= mask
            R[0] <<= 1
            for d in xrange(1, k+1): 
                tmp = R[d]
                R[d] = (old_Rd1 & (R[d] | mask)) << 1
                old_Rd1 = tmp
            for k2 in xrange(k + 1):
                if 0 == (R[k2] & (1<<m)):
                    result = (k2, i - m + 1, 1.0 - float(k2)/m)
                    break
        return result

    def substringOf(self, text, k=None, pad=" "):
        return self.search(text, k, pad)[1] 

def getLevenshteinDistance(s, t):
    n, m = len(s), len(t)
    
    if n == 0 or m == 0: 
        return n or m

    p = [i for i in xrange(n + 1)]
    d = [0]*(n + 1)

    for j in xrange(m):
        t_j = t[j]
        d[0] = j + 1
        for i in xrange(n):
            cost = s[i] != t_j and 1 or 0
            d[i + 1] = min(min(d[i] + 1, p[i + 1] + 1), p[i] + cost)
        p, d = d, p

    return p[n]

if __name__ == '__main__':
    p = FuzzyBitapPattern("orlov")
    for t, k in [
        ("or  lov", 5),
        ("orl  ov", 5),
        ("  or lov ", 5),
        ("orl", 5),
    ]:
        #r = p.search2(t, k)
        #print r, '=', t, p.pattern, k
        dist, pos, prob = p.search(t)
        print 'search=', (dist, pos, prob), ', match=', p.match(t)
        print 'getLevenshteinDistance=', getLevenshteinDistance(p.pattern, t) 
