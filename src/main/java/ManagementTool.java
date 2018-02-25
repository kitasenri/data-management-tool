
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.extensions.java6.auth.oauth2.AuthorizationCodeInstalledApp;
import com.google.api.client.extensions.jetty.auth.oauth2.LocalServerReceiver;
import com.google.api.client.googleapis.auth.oauth2.GoogleAuthorizationCodeFlow;
import com.google.api.client.googleapis.auth.oauth2.GoogleClientSecrets;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.util.store.FileDataStoreFactory;
import com.google.api.services.sheets.v4.Sheets;
import com.google.api.services.sheets.v4.SheetsScopes;

/**
 * GoogleDrive上のスプレッドシートに商品情報を入力していただき、それをSQLに変換するためのプログラムです。
 *
 * ・スプレッドシートURL
 * https://docs.google.com/spreadsheets/d/1DLO7OK62WbHYX3npG_XvyCrVcdpn5rA-4-WQDR-ZelI/edit?usp=sharing
 *
 * ・アクセスアカウント
 */
public class ManagementTool {

    //--------------------------------------------------------------------------
    // Properties
    //--------------------------------------------------------------------------
    // spreadsheet id
    private static final String SPREADSHEET_ID = "1DLO7OK62WbHYX3npG_XvyCrVcdpn5rA-4-WQDR-ZelI";

    // Prints the names and majors of students in a sample spreadsheet:
    // https://docs.google.com/spreadsheets/d/1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms/edit
    private static final String RESOURCES_DIR = "./src/main/resources/";
    private static final String CREDIDENT_FILE = "client_secret.json";

    private static final String PREFIX_COLUMN = "`";
    private static final String PREFIX_VALUE = "'";

    /** 対象となるシート一覧 */
    private static final String[] RANGES = {
        "企業情報#t_shop!A1:D",
        "商品情報#t_shop_product!A1:D"
    };

    private static final String APPLICATION_NAME = "MyApplication";

    /** SQLを保存するPath */
    private static final File DATA_STORE_DIR = new File(
        System.getProperty("user.home"),
        ".credentials/sheets.googleapis.com-java-quickstart"
    );

    private static FileDataStoreFactory DATA_STORE_FACTORY;
    private static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();
    private static HttpTransport HTTP_TRANSPORT;

    private static final List<String> SCOPES =
        Arrays.asList(SheetsScopes.SPREADSHEETS_READONLY);

    static {
        try {
            HTTP_TRANSPORT = GoogleNetHttpTransport.newTrustedTransport();
            DATA_STORE_FACTORY = new FileDataStoreFactory(DATA_STORE_DIR);
        } catch (Throwable t) {
            t.printStackTrace();
            System.exit(1);
        }
    }

    //--------------------------------------------------------------------------
    // Methods
    //--------------------------------------------------------------------------
    /**
     * OAuth認証まわり
     * Googleのサンプルから持ってきたもの
     */
    public static Credential authorize() throws IOException {

        InputStream in = new FileInputStream(RESOURCES_DIR + CREDIDENT_FILE);
        GoogleClientSecrets clientSecrets = GoogleClientSecrets.load(JSON_FACTORY, new InputStreamReader(in));

        GoogleAuthorizationCodeFlow flow = new GoogleAuthorizationCodeFlow.Builder(HTTP_TRANSPORT, JSON_FACTORY, clientSecrets, SCOPES)
                                               .setDataStoreFactory(DATA_STORE_FACTORY)
                                               .setAccessType("offline")
                                               .build();

        Credential credential = new AuthorizationCodeInstalledApp(flow, new LocalServerReceiver())
                                    .authorize("user");

        System.out.println("Credentials saved to " + DATA_STORE_DIR.getAbsolutePath());
        return credential;
    }

    /**
     * Sheetサービスの生成
     * Googleのサンプルから持ってきたもの。
     */
    public static Sheets getSheetsService() throws IOException {
        Credential credential = authorize();
        return new Sheets.Builder(HTTP_TRANSPORT, JSON_FACTORY, credential)
                   .setApplicationName(APPLICATION_NAME)
                   .build();
    }

    /**
     * Column文字列に。
     */
    private static String toColumn( String column ) {
        return PREFIX_COLUMN + column + PREFIX_COLUMN;
    }

    /**
     * 値に。
     */
    private static String toValue( String value ) {

        String retval = null;
        if ( "".equals(value) != true ) {
            String tempValue = value.replaceAll("\\\\", "¥");
            tempValue = tempValue.replaceAll("\'", "\\\\\\'");
            retval = PREFIX_VALUE + tempValue + PREFIX_VALUE;
        }

        return retval;
    }

    /**
     * 一個分のSQLを生成
     */
    protected static void createSQLFile( Sheets service, String range ) throws Exception {

        StringBuilder sql = new StringBuilder();
        String tableName = range.split("!")[0].split("#")[1];
        String filename = tableName + ".sql";
        List<List<Object>> cells = service.spreadsheets().values().get(SPREADSHEET_ID, range).execute().getValues();

        sql.append("INSERT INTO `").append(tableName).append("` (");

        // (1) columns
        List<Object> headers = cells.get(0);
        int headerLength = headers.size();
        String[] headerBuffer = new String[ headerLength ];
        IntStream.range(0, headerLength).forEach(
            ii -> headerBuffer[ii] = toColumn( headers.get(ii).toString().split("#")[1] )
        );

        sql.append( String.join(",", headerBuffer) )
           .append(") VALUES ");

        // (2) values
        String[] values = new String[cells.size() - 1];
        for ( int ii = 1 ; ii < cells.size() ; ii++ ) {

            List<Object> cols = cells.get(ii);
            String[] bodyBuffer = new String[ headerLength ];
            IntStream.range(0, cols.size()).forEach(
                jj -> bodyBuffer[jj] = toValue( cols.get(jj).toString() )
            );

            values[ii - 1] = "(" + String.join(",", bodyBuffer) + ")";
        }

        sql.append( String.join(",", values) )
           .append(";");

        File f = new File(RESOURCES_DIR + filename);
        if ( f.exists() ) { f.delete(); }

        PrintWriter pw = null;
        try {

            FileWriter fw = new FileWriter(RESOURCES_DIR + filename);
            pw = new PrintWriter(new BufferedWriter(fw));
            pw.println( sql.toString() );

            System.out.println("Complete : " + filename);

        } catch ( Throwable ee ) {
            ee.printStackTrace();
        } finally {

            if ( pw != null ) {
                pw.close();
            }

        }

    }

    //--------------------------------------------------------------------------
    // Main文
    //--------------------------------------------------------------------------
    public static void main(String[] args) throws Exception {

        Sheets service = getSheetsService();
        for ( String range : RANGES ) {
            createSQLFile( service, range );
        }

    }

}