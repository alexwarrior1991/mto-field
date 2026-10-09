package com.alejandro.mtofield.configuration.scheduling;

import com.alejandro.mtofield.application.service.PossessionBoardService;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

/**
 * Lo que corre solo: el tic del tablero ({@code app.field.board.tick}), que marca sucias las
 * posesiones con observadores para que la vida de los equipos decaiga aunque nadie hable. El
 * barrido de tokens caducados ({@code app.field.token-expiry}) llega en la fase 3.
 */
@Configuration
@EnableScheduling
public class FieldSchedulingConfiguration {

    private final PossessionBoardService board;

    public FieldSchedulingConfiguration(PossessionBoardService board) {
        this.board = board;
    }

    @Scheduled(fixedDelayString = "${app.field.board.tick:5s}", initialDelayString = "${app.field.board.tick:5s}")
    public void boardTick() {
        board.tick();
    }
}
