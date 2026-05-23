package com.example.adevents.service;

import com.example.adevents.model.StaticEnrichment;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.test.util.ReflectionTestUtils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class StaticEnrichmentServiceTest {

    @Test
    void loadsMapAndLooksUpById() {
        String json = """
                {
                  "EVT-1": {"department":"Eng","role":"Dev","site":"BLR","managerEmail":"m@x"},
                  "EVT-2": {"department":"Fin","role":"Ana","site":"BOM","managerEmail":"f@x"}
                }
                """;
        ResourceLoader rl = mock(ResourceLoader.class);
        Resource res = new ByteArrayResource(json.getBytes());
        when(rl.getResource("classpath:static.json")).thenReturn(res);

        StaticEnrichmentService svc = new StaticEnrichmentService(rl, new ObjectMapper());
        ReflectionTestUtils.setField(svc, "location", "classpath:static.json");
        svc.init();

        assertThat(svc.size()).isEqualTo(2);
        StaticEnrichment se = svc.getByEventId("EVT-1");
        assertThat(se).isNotNull();
        assertThat(se.getDepartment()).isEqualTo("Eng");
        assertThat(svc.getByEventId("MISSING")).isNull();
        assertThat(svc.getByEventId(null)).isNull();
    }

    @Test
    void missingResourceKeepsEmptyMap() {
        ResourceLoader rl = mock(ResourceLoader.class);
        Resource res = mock(Resource.class);
        when(res.exists()).thenReturn(false);
        when(rl.getResource("classpath:nope.json")).thenReturn(res);

        StaticEnrichmentService svc = new StaticEnrichmentService(rl, new ObjectMapper());
        ReflectionTestUtils.setField(svc, "location", "classpath:nope.json");
        svc.init();

        assertThat(svc.size()).isZero();
        assertThat(svc.getByEventId("X")).isNull();
    }
}
