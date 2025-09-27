package bt7s7k7.vinf_project.common;

public final class Logger {
	private Logger() {}

	public static void text(String msg) {
		System.out.println(msg);
	}

	public static void success(String msg) {
		System.out.println("\u001b[92m" + msg + "\u001b[0m");
	}

	public static void info(String msg) {
		System.out.println("\u001b[36m" + msg + "\u001b[0m");
	}

	public static void warn(String msg) {
		System.out.println("\u001b[93m" + msg + "\u001b[0m");
	}

	public static void error(String msg) {
		System.out.println("\u001b[91m" + msg + "\u001b[0m");
	}
}
