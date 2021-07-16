public class MainActivity extends AppCompatActivity {

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_main);

        Timber.d("onCreate Called");
    }

    @Override
    protected void onResume() {
        super.onResume();

        Timber.d("OnResume Called");

    }

    @Override
    protected void onPause() {
        super.onPause();

        Timber.d("onPause Called");
    }

    @Override
    protected void onStart() {
        super.onStart();

        Timber.d("onStart Called");
    }

    @Override
    protected void onStop() {
        super.onStop();

        Timber.d("onStop Called");
    }

    @Override
    protected void onDestroy() {
        super.onDestroy();

        Timber.d("onDestroy called");
    }
}