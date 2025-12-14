package com.igot.cios;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.boot.SpringApplication;

import static org.mockito.Mockito.mockStatic;


@ExtendWith(MockitoExtension.class)
public class CiosContentServiceApplicationTest {

    @Test
    void testMainMethodCallsSpringApplicationRun() {
        try (MockedStatic<SpringApplication> mocked = mockStatic(SpringApplication.class)) {
            CiosContentServiceApplication.main(new String[]{});
            mocked.verify(() -> SpringApplication.run(CiosContentServiceApplication.class, new String[]{}));
        }
    }
}
