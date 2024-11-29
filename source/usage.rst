Usage Guide
===========

Overview
--------

SENDQSARpy simplifies the following workflows:

1. **Data Acquisition**: Fetching and preprocessing data from the SEND database.
2. **Descriptor Calculation**: Computing molecular descriptors for QSAR modeling.
3. **Model Building**: Training predictive models using various algorithms.
4. **Evaluation**: Assessing model performance.

Detailed Example
----------------

This is how you can use SENDQSARpy for a complete QSAR workflow:

.. code-block:: python

   from sendqsarpy import preprocess, calculate_descriptors, train_model

   # Step 1: Preprocess the data
   data = preprocess("path/to/send_data")

   # Step 2: Calculate molecular descriptors
   descriptors = calculate_descriptors(data)

   # Step 3: Train the model
   model = train_model(descriptors)

   # Step 4: Evaluate the model
   results = model.evaluate()

   print("Model Evaluation:", results)
