//package org.marcus.weather.analysis;
//
//import javax.swing.JFrame;
//
//import com.panayotis.gnuplot.JavaPlot;
//import com.panayotis.gnuplot.terminal.SVGTerminal;
//import com.panayotis.iodebug.Debug;
//
//public class JavaPlotTry {
//
//	/**
//	 * @param args
//	 * @throws InterruptedException 
//	 */
//	public static void main(String[] args) throws InterruptedException {
//		JavaPlot p = new JavaPlot();
//		p.getDebugger().setLevel(Debug.CRITICAL);
//		
//		SVGTerminal svg = new SVGTerminal();
//        p.setTerminal(svg);
//
//        p.setTitle("SVG Terminal Title");
//        p.addPlot("x+3");
//        p.plot();
//		
//        try {
//            JFrame f = new JFrame();
//            f.getContentPane().add(svg.getPanel());
//            f.pack();
//            f.setLocationRelativeTo(null);
//            f.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
//            f.setVisible(true);
//        } catch (ClassNotFoundException ex) {
//            System.err.println("Error: Library SVGSalamander not properly installed?");
//        }
//	}
//
//}
