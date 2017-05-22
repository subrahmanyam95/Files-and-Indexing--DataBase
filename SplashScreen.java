
public class SplashScreen{

		public static void main(String[] args) {
			splashScreen();
		}
		
		public static void splashScreen() {
			System.out.println(line("*",80));
			System.out.println("Welcome to DavisBaseLite"); 
			version();
			System.out.println("Type \"help;\" to display supported commands.");
			System.out.println(line("*",80));
		}
	
		
		public static String line(String s,int num) {
			String a = "";
			for(int i=0;i<num;i++) {
				a += s;
			}
			return a;
		}
	
		
		public static void newline(int num) {
			for(int i=0;i<num;i++) {
				System.out.println();
			}
		}
	
		public static void version() {
			System.out.println("DavisBaseLite v1.0\n");
		}

}
