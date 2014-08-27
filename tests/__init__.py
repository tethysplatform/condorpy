import unittest, coverage, sys, time

def runall():
    """

    :rtype : None
    """
    allTests = unittest.TestLoader().discover('')
    unittest.TextTestRunner(verbosity=1).run(allTests)

if __name__ == "__main__":
    cov = coverage.coverage(branch=True, source=['condorpy'])
    cov.start()

    runall()

    cov.stop()
    cov.save()
    #cov.html_report()
    time.sleep(.1)  #because sys.stdout.flush() isn't working
    cov.report()