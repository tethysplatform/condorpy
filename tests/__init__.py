import unittest, coverage

def runall():
    allTests = unittest.TestLoader().discover('')
    unittest.TextTestRunner(verbosity=2).run(allTests)

if __name__ == "__main__":
    cov = coverage.coverage(branch=True, source=['./condorpy'])
    cov.start()

    runall()

    cov.stop()
    cov.save()
    cov.html_report()