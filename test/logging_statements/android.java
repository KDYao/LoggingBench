abstract class BaseAppFragment {

   companion object {
       private const val TAG = "mga.lifecycle";
   }

   override fun onCreate(savedInstanceState: Bundle?) {
       super.onCreate(savedInstanceState);
       Log.d(TAG, "onCreate(): $this");
   }

   override fun onStart() {
       super.onStart();
       Log.d(TAG, "onStart(): $this");
   }

   override fun onResume() {
       super.onResume();
       Log.d(TAG, "onResume(): $this");
   }

   override fun onPause() {
       Log.d(TAG, "onPause(): $this");
       super.onPause();
   }

   override fun onStop() {
       cancelAllSubscriptions();
       Log.d(TAG, "onStop(): $this");
       super.onStop();
   }

   override fun onDestroy() {
       Log.d(TAG, "onDestroy(): $this");
       super.onDestroy();
   }

   override fun onDetach() {
       Log.d(TAG, "onDetach(): $this");
       super.onDetach();
   }
}