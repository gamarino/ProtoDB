Finite State Machine
==================

.. module:: proto_db.fsm

This module provides a Finite State Machine (FSM) implementation for ProtoBase.

FSM
---

.. autoclass:: FSM
   :members:
   :special-members: __init__

The ``FSM`` class is a Finite State Machine implementation. It allows you to define states, transitions between states, and actions to be performed on transitions.

Timer
-----

.. autoclass:: Timer
   :members:
   :special-members: __init__

The ``Timer`` class provides timing functionality for the FSM. It can be used to trigger transitions after a certain amount of time.

Usage Examples
-------------

Basic FSM
~~~~~~~~

.. code-block:: python

    from proto_db.fsm import FSM
    
    # Create an FSM
    fsm = FSM()
    
    # Define states
    fsm.add_state("idle")
    fsm.add_state("running")
    fsm.add_state("paused")
    fsm.add_state("stopped")
    
    # Define transitions
    fsm.add_transition("idle", "running", "start")
    fsm.add_transition("running", "paused", "pause")
    fsm.add_transition("paused", "running", "resume")
    fsm.add_transition("running", "stopped", "stop")
    fsm.add_transition("paused", "stopped", "stop")
    
    # Set the initial state
    fsm.set_state("idle")
    
    # Trigger transitions
    fsm.trigger("start")  # State changes to "running"
    fsm.trigger("pause")  # State changes to "paused"
    fsm.trigger("resume")  # State changes to "running"
    fsm.trigger("stop")   # State changes to "stopped"
    
    # Check the current state
    print(fsm.state)  # Output: stopped

FSM with Actions
~~~~~~~~~~~~~~

.. code-block:: python

    from proto_db.fsm import FSM
    
    # Create an FSM
    fsm = FSM()
    
    # Define states
    fsm.add_state("idle")
    fsm.add_state("running")
    fsm.add_state("paused")
    fsm.add_state("stopped")
    
    # Define actions
    def on_start():
        print("Starting...")
    
    def on_pause():
        print("Pausing...")
    
    def on_resume():
        print("Resuming...")
    
    def on_stop():
        print("Stopping...")
    
    # Define transitions with actions
    fsm.add_transition("idle", "running", "start", on_start)
    fsm.add_transition("running", "paused", "pause", on_pause)
    fsm.add_transition("paused", "running", "resume", on_resume)
    fsm.add_transition("running", "stopped", "stop", on_stop)
    fsm.add_transition("paused", "stopped", "stop", on_stop)
    
    # Set the initial state
    fsm.set_state("idle")
    
    # Trigger transitions
    fsm.trigger("start")  # Prints "Starting..." and changes state to "running"
    fsm.trigger("pause")  # Prints "Pausing..." and changes state to "paused"
    fsm.trigger("resume")  # Prints "Resuming..." and changes state to "running"
    fsm.trigger("stop")   # Prints "Stopping..." and changes state to "stopped"

FSM with Guards
~~~~~~~~~~~~~

.. code-block:: python

    from proto_db.fsm import FSM
    
    # Create an FSM
    fsm = FSM()
    
    # Define states
    fsm.add_state("idle")
    fsm.add_state("running")
    fsm.add_state("paused")
    fsm.add_state("stopped")
    
    # Define a guard condition
    is_authorized = False
    
    def check_authorization():
        return is_authorized
    
    # Define transitions with guards
    fsm.add_transition("idle", "running", "start", guard=check_authorization)
    fsm.add_transition("running", "paused", "pause")
    fsm.add_transition("paused", "running", "resume")
    fsm.add_transition("running", "stopped", "stop")
    fsm.add_transition("paused", "stopped", "stop")
    
    # Set the initial state
    fsm.set_state("idle")
    
    # Try to trigger a transition with a guard that returns False
    fsm.trigger("start")  # State remains "idle" because the guard returns False
    
    # Change the guard condition
    is_authorized = True
    
    # Try again
    fsm.trigger("start")  # State changes to "running" because the guard now returns True

FSM with Timers
~~~~~~~~~~~~~

.. code-block:: python

    import time
    from proto_db.fsm import FSM, Timer
    
    # Create an FSM
    fsm = FSM()
    
    # Define states
    fsm.add_state("idle")
    fsm.add_state("running")
    fsm.add_state("paused")
    fsm.add_state("stopped")
    
    # Define a timer
    timer = Timer()
    
    # Define transitions
    fsm.add_transition("idle", "running", "start")
    fsm.add_transition("running", "paused", "pause")
    fsm.add_transition("paused", "running", "resume")
    fsm.add_transition("running", "stopped", "stop")
    fsm.add_transition("paused", "stopped", "stop")
    
    # Define a timed transition
    def check_timeout():
        return timer.elapsed() > 5.0  # 5 seconds
    
    fsm.add_transition("running", "stopped", "timeout", guard=check_timeout)
    
    # Set the initial state
    fsm.set_state("idle")
    
    # Start the timer and transition to "running"
    timer.start()
    fsm.trigger("start")  # State changes to "running"
    
    # Wait for the timeout
    time.sleep(6)  # Wait for 6 seconds
    
    # Check for timed transitions
    fsm.check_timed_transitions()  # State changes to "stopped" because the timeout guard returns True

Advanced FSM Usage
~~~~~~~~~~~~~~~~

.. code-block:: python

    from proto_db.fsm import FSM
    
    # Create an FSM
    fsm = FSM()
    
    # Define states with entry and exit actions
    def on_enter_idle():
        print("Entering idle state")
    
    def on_exit_idle():
        print("Exiting idle state")
    
    def on_enter_running():
        print("Entering running state")
    
    def on_exit_running():
        print("Exiting running state")
    
    fsm.add_state("idle", on_enter=on_enter_idle, on_exit=on_exit_idle)
    fsm.add_state("running", on_enter=on_enter_running, on_exit=on_exit_running)
    
    # Define transitions with actions and guards
    def on_start():
        print("Starting...")
    
    def check_can_start():
        return True
    
    fsm.add_transition("idle", "running", "start", action=on_start, guard=check_can_start)
    
    # Set the initial state
    fsm.set_state("idle")  # Prints "Entering idle state"
    
    # Trigger a transition
    fsm.trigger("start")  # Prints "Exiting idle state", "Starting...", "Entering running state"
    
    # Get all available transitions from the current state
    transitions = fsm.get_transitions_from_state("running")
    print(transitions)  # Output: List of transitions from "running" state