package com.gojek.mc2pinot.mc;

import com.aliyun.odps.Instance;
import com.aliyun.odps.LogView;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.task.SQLTask;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.logging.Handler;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class MCUnloaderTest {

    private static final String RAW_LOGVIEW_URL =
            "http://logview.odps.aliyun.com/logview/?h=http%3A%2F%2Fservice.ap-southeast-5.maxcompute.aliyun.com%2Fapi&p=my_project&i=abc123";
    private static final String EXPECTED_LOGVIEW_HOST =
            "http://service.id-all.maxcompute.aliyun-inc.com/api";

    @Mock
    private Odps odpsClient;

    @Mock
    private Instance instance;

    @Mock
    private LogView logView;

    private MCUnloader mcUnloader;

    @BeforeEach
    void setUp() throws OdpsException {
        when(odpsClient.getDefaultProject()).thenReturn("test_project");
        when(odpsClient.logview()).thenReturn(logView);
        when(logView.generateLogView(any(Instance.class), anyLong())).thenReturn(RAW_LOGVIEW_URL);
        QueryUnloadBuilder builder = new QueryUnloadBuilder();
        mcUnloader = new MCUnloader(odpsClient, builder, "acs:ram::123:role/testrole", "json");
    }

    @Test
    void shouldExecuteUnloadQuery() throws Exception {
        try (MockedStatic<SQLTask> sqlTaskMock = mockStatic(SQLTask.class)) {
            sqlTaskMock.when(() -> SQLTask.run(eq(odpsClient), eq("test_project"), anyString(), anyMap(), isNull()))
                    .thenReturn(instance);

            mcUnloader.unload("SELECT * FROM t", "oss://bucket/path");

            sqlTaskMock.verify(() -> SQLTask.run(eq(odpsClient), eq("test_project"),
                    contains("UNLOAD FROM"), anyMap(), isNull()));
            verify(instance).waitForSuccess();
        }
    }

    @Test
    void shouldRewriteLogViewHostParam() throws Exception {
        try (MockedStatic<SQLTask> sqlTaskMock = mockStatic(SQLTask.class)) {
            sqlTaskMock.when(() -> SQLTask.run(eq(odpsClient), eq("test_project"), anyString(), anyMap(), isNull()))
                    .thenReturn(instance);

            Handler handler = mock(Handler.class);
            Logger logger = Logger.getLogger(MCUnloader.class.getName());
            logger.addHandler(handler);
            try {
                mcUnloader.unload("SELECT * FROM t", "oss://bucket/path");
            } finally {
                logger.removeHandler(handler);
            }

            ArgumentCaptor<LogRecord> logCaptor = ArgumentCaptor.forClass(LogRecord.class);
            verify(handler, atLeastOnce()).publish(logCaptor.capture());

            String logviewLog = logCaptor.getAllValues().stream()
                    .map(LogRecord::getMessage)
                    .filter(m -> m.startsWith("source(mc): logview:"))
                    .findFirst()
                    .orElseThrow(() -> new AssertionError("logview log line not found"));

            assertTrue(logviewLog.contains("h=" + EXPECTED_LOGVIEW_HOST.replace(":", "%3A").replace("/", "%2F")),
                    "Expected rewritten host in logview URL, got: " + logviewLog);
            assertFalse(logviewLog.contains("ap-southeast-5"),
                    "Original host should be replaced, got: " + logviewLog);
        }
    }

    @Test
    void shouldPropagateOdpsException() throws Exception {
        try (MockedStatic<SQLTask> sqlTaskMock = mockStatic(SQLTask.class)) {
            sqlTaskMock.when(() -> SQLTask.run(eq(odpsClient), eq("test_project"), anyString(), anyMap(), isNull()))
                    .thenReturn(instance);
            doThrow(new OdpsException("task failed")).when(instance).waitForSuccess();

            assertThrows(OdpsException.class, () -> mcUnloader.unload("SELECT 1", "oss://bucket/out"));
        }
    }
}

