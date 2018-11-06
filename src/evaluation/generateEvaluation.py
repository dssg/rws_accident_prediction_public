from evaluation.evaluate import EvaluationResultUpdater
import sys

def main(argv):
    experiment_ids = argv[1:]
    experiment_ids = [int(e) for e in experiment_ids]
    
    for eid in [experiment_ids]:
        updater = EvaluationResultUpdater(eid)
        updater.update_result_tables()

if __name__== '__main__':
    main(sys.argv)
