package feed_filter;

import javax.swing.*;
import java.awt.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

public class BigGainDetectorWindow extends JFrame {

    private final JTextArea logArea;
    private final JLabel statusLabel;
    private final SimpleDateFormat tsFormat;

    public BigGainDetectorWindow() {
        super("Big gain detector");

        this.tsFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss 'UTC'");
        this.tsFormat.setTimeZone(TimeZone.getTimeZone("UTC"));

        this.logArea = new JTextArea();
        logArea.setEditable(false);
        logArea.setLineWrap(false);
        logArea.setFont(new Font(Font.MONOSPACED, Font.PLAIN, 12));
        logArea.setBackground(new Color(20, 22, 28));
        logArea.setForeground(new Color(230, 230, 230));

        JScrollPane scroll = new JScrollPane(logArea);
        scroll.setBorder(BorderFactory.createEmptyBorder());

        this.statusLabel = new JLabel("Last scan: n/a");
        statusLabel.setFont(new Font(Font.MONOSPACED, Font.PLAIN, 11));
        statusLabel.setForeground(new Color(200, 200, 200));
        statusLabel.setBorder(BorderFactory.createEmptyBorder(2, 4, 2, 4));
        statusLabel.setBackground(new Color(20, 22, 28));
        statusLabel.setOpaque(true);

        setLayout(new BorderLayout());
        add(scroll, BorderLayout.CENTER);
        add(statusLabel, BorderLayout.SOUTH);

        setSize(900, 500);
        setLocationRelativeTo(null);
    }

    public void appendAlert(String line) {
        if (line == null || line.isEmpty()) {
            return;
        }
        logArea.append(line);
        if (!line.endsWith("\n")) {
            logArea.append("\n");
        }
        logArea.setCaretPosition(logArea.getDocument().getLength());
    }

    public void setLastScan(long tsMillis, int totalCombinations, int shown) {
        String ts = tsFormat.format(new Date(tsMillis));
        String text = String.format(
                "Last scan: %s | scanned: %d combinations | shown: %d",
                ts,
                totalCombinations,
                shown
        );
        statusLabel.setText(text);
    }
}
