Getting Started
===============

Installation
------------

You can install SENDQSARpy using pip:

.. code-block:: bash

   pip install sendqsarpy

Quick Start
-----------

Here’s an example of how to get started:

.. code-block:: python

   from sendqsarpy import main_function

   # Preprocess the SEND data
   processed_data = main_function("path/to/send_data")

   # Train a QSAR model
   model = processed_data.train_model()

   # Evaluate the model
   results = model.evaluate()

   print(results)
