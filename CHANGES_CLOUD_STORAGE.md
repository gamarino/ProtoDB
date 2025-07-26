# Changes to Fix CloudFileStorage Test Hanging Issue

## Problem Description
The test for `cloud_file_storage.py` was hanging indefinitely (taking more than 10 minutes to complete). After analyzing the code, I identified that the issue was related to the background uploader thread in the `CloudFileStorage` class.

## Root Cause
The background uploader thread in `CloudFileStorage` runs in an infinite loop as long as `self.state == 'Running'`. However, there was no mechanism to properly stop this thread when the storage was closed, causing the test to hang indefinitely.

## Changes Made

1. Added an `uploader_running` flag to control the background uploader thread:
   ```python
   # Start background uploader thread
   self.uploader_running = True
   self.uploader_thread = threading.Thread(target=self._background_uploader)
   self.uploader_thread.daemon = True
   self.uploader_thread.start()
   ```

2. Modified the `_background_uploader` method to check both the `state` and the `uploader_running` flag:
   ```python
   def _background_uploader(self):
       """
       Background thread for uploading pending writes to S3.
       """
       while self.state == 'Running' and self.uploader_running:
           try:
               # Sleep for the upload interval
               time.sleep(self.upload_interval_ms / 1000)
               
               # Process pending uploads
               self._process_pending_uploads()
           except Exception as e:
               _logger.error(f"Error in background uploader: {e}")
   ```

3. Updated the `close` method to properly stop the background uploader thread:
   ```python
   def close(self):
       """
       Closes the storage, flushing any pending writes and releasing resources.
       
       This method overrides the base implementation to also process pending uploads
       before closing.
       """
       # Process pending uploads
       self._process_pending_uploads()
       
       # Stop the background uploader thread
       self.uploader_running = False
       
       # Wait for the uploader thread to finish
       if hasattr(self, 'uploader_thread') and self.uploader_thread.is_alive():
           self.uploader_thread.join(timeout=2.0)  # Wait up to 2 seconds for the thread to finish
       
       # Call the parent implementation
       super().close()
       
       _logger.info("Closed CloudFileStorage")
   ```

## Expected Outcome
These changes should ensure that the background uploader thread is properly stopped when the storage is closed, which should fix the issue with the test hanging indefinitely.

The test now checks for `self.assertFalse(self.storage.uploader_running)` to verify that the uploader thread has been stopped, which should now pass with these changes.