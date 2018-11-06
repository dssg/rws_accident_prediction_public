# Evaluation

## How we are thinking about Precision and Recall

Framing precision/recall curves in terms of % of population, rather than threshold, is usually done to allow the evaluation to be based on a policy action. By optimizing on the precision on a specific % of population, we aim to optimize on the ultimate action that will be taken with the model.

For this problem, our validation set consists of a list of physical spaces as seen at many times. But that shouldn't be treated as our 'population', as no policy or action can be taken over all spaces and all times. The bottleneck of resources is that at any specific time there are a specific number of inspectors to be allocated. So the two dimensions of our population of observations are separated. At each time, we set a threshold of % of the road segments to classify as positive.

That structure means that we will predict the same number of positives for each time, which artificially deflates our precision (as we are forcing ourselves to select positives even when it is unlikely there are any) but also ensures that our prediction is made in a way that action can be taken on our results at all times.

For now we are separately calculating precision recall curves for the daytime shift, the nighttime shift, and overall. This is important since we care about precision @ 8 during the daytime hours, and precision @ 1 during nighttime hours, as that is the number of inspectors on duty at each time.

## What these files do wrt that

generateEvaluation.py - This is what you would call after running orchestra. Passing it the list of experiment_ids that have completed, it would fill the results and raw_y tables. The results table is filled specifically with recall and precision (as defined above) for 8 during the day and for 1 at night.

report_generator.py - This is what you would call after running generateEvaluation.py in order to generate plots showing the precision and recall curves. It generates a pdf report for every model, for every parameter set it was run with for the experiment. 

evaluate.py - Called by generateEvaluation.py

visualize.py - Called by report_generator.py

