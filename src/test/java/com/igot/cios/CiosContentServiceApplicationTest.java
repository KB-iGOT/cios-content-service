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
        // Mock the static SpringApplication.run() call
        try (MockedStatic<SpringApplication> mocked = mockStatic(SpringApplication.class)) {

            // Call the main() method
            CiosContentServiceApplication.main(new String[]{});

            // Verify that SpringApplication.run() was invoked correctly
            mocked.verify(() -> SpringApplication.run(CiosContentServiceApplication.class, new String[]{}));
        }
    }
}
