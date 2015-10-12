package tfidf;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ExeTFIDF {

	public static void main(String[] args) {
		long startTime = System.currentTimeMillis();
		exe();
		System.out.println((System.currentTimeMillis() - startTime) + "ms");
	}

	private static void exe() {
		ExecutorService thread = Executors.newFixedThreadPool(10);

		for (int i = 0; i < 10; i++) {
			thread.submit(new TFIDF(new Integer(i) + ".txt"));
		}
		thread.shutdown();
		while (!thread.isTerminated())
			;
	}

	private static class TFIDF implements Runnable {
		private static Map<String, Integer> dfMap = new ConcurrentHashMap<String, Integer>();

		private static final String code = "MS932";

		private static final CyclicBarrier cyclicBarrier = new CyclicBarrier(10);

		private String filename;

		public TFIDF(String filename) {
			this.filename = filename;
		}

		public void run() {
			Map<String, Integer> tfMap = reader();
			try {
				cyclicBarrier.await();
			} catch (Exception e) {
				e.printStackTrace();
			}
			writer(tfMap);
		}

		private Map<String, Integer> reader() {
			String word;
			Map<String, Integer> tfMap = new HashMap<String, Integer>();
			Set<String> dfSet = new HashSet<String>();
			try {
				BufferedReader br = new BufferedReader(
						new InputStreamReader(new FileInputStream("C:/input/" + filename), code));
				while ((word = br.readLine()) != null) {
					if (!dfSet.contains(word)) {
						dfSet.add(word);
						dfMap.put(word, dfMap.containsKey(word) ? dfMap.get(word) + 1 : 1);
					}
					tfMap.put(word, tfMap.containsKey(word) ? tfMap.get(word) + 1 : 1);
				}
				br.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
			return tfMap;
		}

		private void writer(Map<String, Integer> tfMap) {
			try {
				BufferedWriter bw = new BufferedWriter(
						new OutputStreamWriter(new FileOutputStream("C:/output/" + filename)));
				String key;
				StringBuffer strBuf = new StringBuffer();
				for (Map.Entry<String, Integer> entry : tfMap.entrySet()) {
					key = entry.getKey();
					strBuf.append(key + "\t" + entry.getValue() * Math.log((double) 10 / dfMap.get(key)) + "\n");
				}
				bw.write(strBuf.toString());
				bw.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
}
