import unittest

def runall():
    allTests = unittest.TestLoader().discover('')
    unittest.TextTestRunner(verbosity=2).run(allTests)

if __name__ == "__main__":
    runall()