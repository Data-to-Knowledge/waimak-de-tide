Remove the tidal effect on the Waimak SH1 water levels
=================================================================

This git repository contains code necessary to create a flow time series in Hydrotel for lowflow restrictions at the Old Highway Bridge on the mouth of the Waimakariri River.

There are four main procedures:
  1. Create the appropriate dataset names in Hydrotel (i.e. create the object and point numbers)
  2. Remove the tides from the monitored flow (detided flow)
  3. Extract the abstractions and add them back to the detided flow (unmodified flow)
  4. Assign the unmodified flow Hydrotel name to the appropriate lowflow band(s)

Python environment
------------------
The python environment with the required python packages to run all of the scripts are specified in the `main.yml file <https://github.com/mullenkamp/waimak-de-tide/blob/master/scripts/main.yml>`_. A `batch file <https://github.com/mullenkamp/waimak-de-tide/blob/master/scripts/install_env.bat>`_ has been created to automatically install the python environment and must be run with admin rights. Similarly, there is a `remove_env.bat file <https://github.com/mullenkamp/waimak-de-tide/blob/master/scripts/remove_env.bat>`_ to remove the environment.

Detailed procedure
------------------
The first three procedures are laid out in the below figure:

.. image:: waimak_unmod_flow.png

Each of these three procedures are represented at three python scripts. The first procedure only needs to be run once per Hydrotel installation. Once the datasets have been created, the appropriate fields need to be populated in the `parameters.yml file <https://github.com/mullenkamp/waimak-de-tide/blob/master/scripts/parameters.yml>`_ (e.g. detided_mtype, unmod_mtype, other_mtype, detided_point, unmod_point, other_point).

Once the parameters.yml is filled, then procedures 2 and 3 are run in sequence daily. These procedures are contained within the waimak_detide.py and waimak_unmod_flow.py files respectively. The main.py file orchestrates the run sequence and the main.bat file runs the main.py in the appropriate python environment for Windows OS.

Update 2020-10-01
-------------------
The FlowNaturalisation dependencies have been removed. Procedure 3 now uses a static dataset of consents and WAPs to determine who is above OHB. This was done to reduce the likelihood of failures do to several dependencies. This dataset should be updated once a year to ensure new consents are captured and old consents are removed.
